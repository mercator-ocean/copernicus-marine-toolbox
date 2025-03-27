import logging
import pathlib
import shutil
from collections import defaultdict

import pandas as pd
from arcosparse import (
    UserConfiguration,
    get_entities_ids,
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
)
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.sessions import TRUST_ENV
from copernicusmarine.core_functions.utils import (
    construct_query_params_for_marine_data_store_monitoring,
    get_unique_filepath,
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


# TODO: should we support necdf?
# https://stackoverflow.com/questions/46476920/xarray-writing-to-netcdf-from-pandas-dimension-issue # noqa
def download_sparse(
    username: str,
    subset_request: SubsetRequest,
    metadata_url: str,
    service: CopernicusMarineService,
    axis_coordinate_id_mapping: dict[str, str],
    disable_progress_bar: bool,
) -> ResponseSubset:

    if subset_request.dry_run:
        _, variables, platform_ids = _read_dataframe_sparse(
            username,
            subset_request,
            metadata_url,
            service,
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
    disable_progress_bar: bool,
) -> pd.DataFrame:
    df, _, _ = _read_dataframe_sparse(
        username,
        subset_request,
        metadata_url,
        service,
        disable_progress_bar,
    )
    return df


def _read_dataframe_sparse(
    username: str,
    subset_request: SubsetRequest,
    metadata_url: str,
    service: CopernicusMarineService,
    disable_progress_bar: bool,
    dry_run: bool = False,
) -> tuple[pd.DataFrame, list[str], list[str]]:
    """
    Shared function for subset and read dataframe

    Returns also the variables and the platform_ids
    """
    user_configuration = _get_user_configuration(username)
    if subset_request.platform_ids:
        platform_ids = _get_plaform_ids_to_subset(
            subset_request.platform_ids or [],
            metadata_url,
            service,
            user_configuration,
        )
    else:
        platform_ids = []
    variables = subset_request.variables or [
        variable.short_name for variable in service.variables
    ]
    if dry_run:
        return pd.DataFrame(), variables, platform_ids
    return (
        subset_and_return_dataframe(
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
        ),
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
    metadata_url: str,
    retrieval_service: CopernicusMarineService,
    user_configuration: UserConfiguration,
) -> list[str]:
    platforms_to_subset = []
    if platform_ids:
        platforms_names = get_entities_ids(metadata_url, user_configuration)
        if not platforms_names:
            raise NotEnoughPlatformMetadata()
        platforms_names_with_types: set[str] = set()
        platforms_without_types_mapping: dict[str, list] = defaultdict(list)
        for platform_name in platforms_names:
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
