import logging
import pathlib
import shutil
import warnings
from collections import defaultdict
from copy import deepcopy
from typing import Optional

import pandas as pd
from arcosparse import (
    Entity,
    UserConfiguration,
    get_entities,
    subset_and_return_dataframe,
)

from copernicusmarine.catalogue_parser.models import CopernicusMarineService
from copernicusmarine.core_functions.environment_variables import (
    COPERNICUSMARINE_DISABLE_SSL_CONTEXT,
    COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH,
)
from copernicusmarine.core_functions.exceptions import (
    NotEnoughPlatformMetadata,
    WrongPlatformID,
)
from copernicusmarine.core_functions.models import (  # TimeExtent,
    FileStatus,
    ResponseSubset,
    StatusCode,
    StatusMessage,
    VerticalAxis,
)
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.sessions import TRUST_ENV
from copernicusmarine.core_functions.utils import (
    construct_query_params_for_marine_data_store_monitoring,
    datetime_to_isoformat,
    get_unique_filepath,
    timestamp_parser,
)
from copernicusmarine.download_functions.utils import (
    build_filename_from_request,
    get_file_extension,
)

logger = logging.getLogger("copernicusmarine")

COLUMNS_RENAME = {
    "entity_id": "platform_id",
    "entity_type": "platform_type",
}

COLUMNS_ORDER_DEPTH = [
    "variable",
    "platform_id",
    "platform_type",
    "time",
    "longitude",
    "latitude",
    "depth",
    "pressure",
    "is_depth_from_producer",
    "value",
    "value_qc",
    "institution",
    "doi",
    "product_doi",
]

COLUMNS_ORDER_ELEVATION = deepcopy(COLUMNS_ORDER_DEPTH)
COLUMNS_ORDER_ELEVATION[COLUMNS_ORDER_ELEVATION.index("depth")] = "elevation"

SORTING = {
    "variable": True,
    "platform_id": True,
    "platform_type": True,
    "time": True,
}


# TODO: should we support necdf?
# https://stackoverflow.com/questions/46476920/xarray-writing-to-netcdf-from-pandas-dimension-issue # noqa
def download_sparse(
    username: str,
    subset_request: SubsetRequest,
    metadata_url: str,
    service: CopernicusMarineService,
    axis_coordinate_id_mapping: dict[str, str],
    product_doi: Optional[str],
    disable_progress_bar: bool,
) -> ResponseSubset:

    if subset_request.dry_run:
        _, variables, platform_ids = _read_dataframe_sparse(
            username,
            subset_request,
            metadata_url,
            service,
            product_doi,
            disable_progress_bar,
            dry_run=True,
        )
        response = _get_response_subset(
            subset_request,
            variables,
            platform_ids,
            axis_coordinate_id_mapping,
        )
        response.status = StatusCode.DRY_RUN
        response.message = StatusMessage.DRY_RUN
        return response

    df, variables, platform_ids = _read_dataframe_sparse(
        username,
        subset_request,
        metadata_url,
        service,
        product_doi,
        disable_progress_bar,
    )
    response = _get_response_subset(
        subset_request,
        variables,
        platform_ids,
        axis_coordinate_id_mapping,
    )
    output_path = response.file_path
    if subset_request.skip_existing and output_path.exists():
        response.file_status = FileStatus.IGNORED
        return response
    elif (
        subset_request.overwrite
        and output_path.exists()
        and output_path.is_dir()
    ):
        shutil.rmtree(output_path)

    if subset_request.output_directory:
        subset_request.output_directory.mkdir(parents=True, exist_ok=True)
    if subset_request.file_format == "parquet":
        df.to_parquet(output_path, index=False)
    else:
        df.to_csv(output_path, index=False)

    return response


def read_dataframe_sparse(
    username: str,
    subset_request: SubsetRequest,
    metadata_url: str,
    service: CopernicusMarineService,
    product_doi: Optional[str],
    disable_progress_bar: bool,
) -> pd.DataFrame:
    df, _, _ = _read_dataframe_sparse(
        username,
        subset_request,
        metadata_url,
        service,
        product_doi,
        disable_progress_bar,
    )
    return df


def _read_dataframe_sparse(
    username: str,
    subset_request: SubsetRequest,
    metadata_url: str,
    service: CopernicusMarineService,
    product_doi: Optional[str],
    disable_progress_bar: bool,
    dry_run: bool = False,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Shared function for subset and read dataframe

    Returns also the variables and the platform_ids
    """
    user_configuration = _get_user_configuration(username)
    platforms_metadata = {
        entity.entity_id: entity
        for entity in get_entities(metadata_url, user_configuration)
    }
    if subset_request.platform_ids:
        platform_ids = _get_plaform_ids_to_subset(
            subset_request.platform_ids or [],
            set(platforms_metadata.keys()),
            service,
        )
    else:
        platform_ids = []
    variables = subset_request.variables or [
        variable.short_name for variable in service.variables
    ]
    if dry_run:
        return pd.DataFrame(), variables, platform_ids
    # see https://github.com/pandas-dev/pandas/issues/55928
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=FutureWarning)
        df = subset_and_return_dataframe(
            minimum_latitude=subset_request.minimum_y,
            maximum_latitude=subset_request.maximum_y,
            minimum_longitude=subset_request.minimum_x,
            maximum_longitude=subset_request.maximum_x,
            minimum_elevation=(
                -subset_request.maximum_depth
                if subset_request.maximum_depth is not None
                else None
            ),
            maximum_elevation=(
                -subset_request.minimum_depth
                if subset_request.minimum_depth is not None
                else None
            ),
            minimum_time=(
                subset_request.start_datetime.timestamp()
                if subset_request.start_datetime is not None
                else None
            ),
            maximum_time=(
                subset_request.end_datetime.timestamp()
                if subset_request.end_datetime is not None
                else None
            ),
            variables=variables,
            entities=platform_ids,
            vertical_axis=subset_request.vertical_axis,
            url_metadata=metadata_url,
            user_configuration=user_configuration,
            disable_progress_bar=disable_progress_bar,
            columns_rename=COLUMNS_RENAME,
        )

        df = _transform_dataframe(
            df,
            subset_request.vertical_axis,
            platforms_metadata,
            product_doi,
        )
    if df.empty:
        logger.info(
            "No data found for the given parameters. "
            "Please check your request and try again."
        )
    return (
        df,
        variables,
        platform_ids,
    )


def _get_user_configuration(username: str) -> UserConfiguration:
    return UserConfiguration(
        disable_ssl=COPERNICUSMARINE_DISABLE_SSL_CONTEXT == "True",
        trust_env=TRUST_ENV,
        ssl_certificate_path=COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH,
        max_concurrent_requests=20,
        extra_params=construct_query_params_for_marine_data_store_monitoring(
            username
        ),
    )


def _get_plaform_ids_to_subset(
    platform_ids: list[str],
    platforms_metadata_names: set[str],
    retrieval_service: CopernicusMarineService,
) -> list[str]:
    platforms_to_subset = []
    if not platforms_metadata_names:
        raise NotEnoughPlatformMetadata()
    platforms_names_with_types: set[str] = set()
    platforms_without_types_mapping: dict[str, list] = defaultdict(list)
    for platform_name in platforms_metadata_names:
        platform_name_without_type = platform_name.split("___")[0]
        platforms_without_types_mapping[platform_name_without_type].append(
            platform_name
        )
        platforms_names_with_types.add(platform_name)
    for platform_id in platform_ids:
        if platform_id in platforms_names_with_types:
            platforms_to_subset.append(platform_id)
        if platform_id in platforms_without_types_mapping:
            platforms_to_subset.extend(
                platforms_without_types_mapping[platform_id]
            )
    if not platforms_to_subset:
        raise WrongPlatformID(
            platform_ids, retrieval_service.platforms_metadata
        )
    return platforms_to_subset


def _build_filename_and_output_path(
    subset_request: SubsetRequest,
    variables: list[str],
    platform_ids: list[str],
    axis_coordinate_id_mapping: dict[str, str],
) -> tuple[pathlib.Path, pathlib.Path]:
    extension_file = get_file_extension(subset_request.file_format)
    filename = pathlib.Path(
        subset_request.output_filename
        or build_filename_from_request(
            subset_request,
            variables,
            platform_ids,
            axis_coordinate_id_mapping,
        )
    )

    if filename.suffix != extension_file:
        filename = pathlib.Path(f"{filename}{extension_file}")
    output_path = pathlib.Path(
        subset_request.output_directory,
        filename,
    )
    if not subset_request.overwrite and not subset_request.skip_existing:
        output_path = get_unique_filepath(output_path)
    return filename, output_path


def _get_response_subset(
    subset_request: SubsetRequest,
    variables: list[str],
    platform_ids: list[str],
    axis_coordinate_id_mapping: dict[str, str],
) -> ResponseSubset:
    filename, output_path = _build_filename_and_output_path(
        subset_request,
        variables,
        platform_ids,
        axis_coordinate_id_mapping,
    )
    return ResponseSubset(
        file_path=output_path,
        output_directory=subset_request.output_directory,
        filename=str(filename),
        file_size=None,
        data_transfer_size=None,
        variables=variables,
        # TODO: handle thoses extents maybe opening the dataframe
        coordinates_extent=[],
        status=StatusCode.SUCCESS,
        message=StatusMessage.SUCCESS,
        file_status=FileStatus.DOWNLOADED,
    )


def _transform_dataframe(
    df: pd.DataFrame,
    vertical_axis: VerticalAxis,
    platforms_metadata: dict[str, Entity],
    product_doi: Optional[str],
) -> pd.DataFrame:
    """
    Transform the dataframe to match the expected format to be consistent with MyOceanPro
    and Copernicus Marine Services.
    """  # noqa
    if df.empty:
        return df
    # Needs to be done before striping the type to the platform_id
    df["institution"] = df["platform_id"].apply(
        lambda x: (
            (
                platforms_metadata[x].institution
                if x in platforms_metadata
                else pd.NA
            )
            or pd.NA
        )
    )
    df["doi"] = df["platform_id"].apply(
        lambda x: (
            (platforms_metadata[x].doi if x in platforms_metadata else pd.NA)
            or pd.NA
        )
    )

    df["product_doi"] = (
        f"https://doi.org/{product_doi}"
        if product_doi and "https://" not in product_doi
        else product_doi
    )

    # From "platform___type" to "platform" since the type is in
    # column platform_type
    df["platform_id"] = df["platform_id"].str.split("___").str[0]

    df["time"] = df["time"].apply(
        lambda x: datetime_to_isoformat(timestamp_parser(x, unit="s"))
    )

    # Some depth values comes from the arcoification of the data
    # and are calculated from the pressure some others come
    # directly from the producer (ie native/original data)
    df["is_depth_from_producer"] = df["is_approx_elevation"].apply(
        lambda x: 0 if x else 1
    )
    df.drop(columns=["is_approx_elevation"], inplace=True)

    if vertical_axis == "elevation":
        df = df[COLUMNS_ORDER_ELEVATION]
    else:
        df = df[COLUMNS_ORDER_DEPTH]

    df.sort_values(
        by=list(SORTING.keys()),
        ascending=list(SORTING.values()),
        inplace=True,
    )

    df.reset_index(drop=True, inplace=True)
    return df
