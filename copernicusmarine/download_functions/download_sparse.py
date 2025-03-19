import logging
import pathlib

import pandas as pd
from arcosparse import (
    UserConfiguration,
    get_platforms_names,
    subset_and_return_dataframe,
    subset_and_save,
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
from copernicusmarine.download_functions.utils import get_file_extension

logger = logging.getLogger("copernicusmarine")


# TODO: should we support necdf?
# https://stackoverflow.com/questions/46476920/xarray-writing-to-netcdf-from-pandas-dimension-issue # noqa
def download_sparse(
    username: str,
    subset_request: SubsetRequest,
    metadata_url: str,
    service: CopernicusMarineService,
    disable_progress_bar: bool,
) -> ResponseSubset:
    user_configuration = _get_user_configuration(username)
    _check_plaform_ids(
        subset_request.platform_ids or [],
        metadata_url,
        service,
        user_configuration,
    )
    extension_file = get_file_extension(
        subset_request.file_format or "parquet"
    )
    filename = pathlib.Path(
        subset_request.output_filename
        or f"{subset_request.dataset_id}_subset{extension_file}"
    )

    if filename.suffix != extension_file:
        filename = pathlib.Path(f"{filename}{extension_file}")
    output_path = pathlib.Path(
        subset_request.output_directory,
        filename,
    )
    if not subset_request.overwrite and not subset_request.skip_existing:
        output_path = get_unique_filepath(output_path)
    variables = subset_request.variables or [
        variable.short_name for variable in service.variables
    ]
    response = ResponseSubset(
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

    if subset_request.dry_run:
        response.status = StatusCode.DRY_RUN
        response.message = StatusMessage.DRY_RUN
        return response
    elif subset_request.skip_existing and output_path.exists():
        response.file_status = FileStatus.IGNORED
        return response

    kwargs = {
        "url_metadata": metadata_url,
        "minimum_latitude": subset_request.minimum_y,
        "maximum_latitude": subset_request.maximum_y,
        "minimum_longitude": subset_request.minimum_x,
        "maximum_longitude": subset_request.maximum_x,
        "maximum_elevation": (
            -subset_request.minimum_depth
            if subset_request.minimum_depth is not None
            else None
        ),
        "minimum_elevation": (
            -subset_request.maximum_depth
            if subset_request.maximum_depth is not None
            else None
        ),
        "minimum_time": (
            subset_request.start_datetime.timestamp()
            if subset_request.start_datetime is not None
            else None
        ),
        "maximum_time": (
            subset_request.end_datetime.timestamp()
            if subset_request.end_datetime is not None
            else None
        ),
        "variables": variables,
        "entities": subset_request.platform_ids or [],
        "vertical_axis": subset_request.vertical_axis,
        "user_configuration": user_configuration,
        "disable_progress_bar": disable_progress_bar,
    }
    if subset_request.file_format == "parquet":
        kwargs["output_path"] = output_path
        subset_and_save(**kwargs)
    else:
        df = subset_and_return_dataframe(**kwargs)
        df.to_csv(output_path, index=False)

    return response


def read_dataframe_sparse(
    username: str,
    subset_request: SubsetRequest,
    metadata_url: str,
    service: CopernicusMarineService,
    disable_progress_bar: bool,
) -> pd.DataFrame:
    user_configuration = _get_user_configuration(username)
    _check_plaform_ids(
        subset_request.platform_ids or [],
        metadata_url,
        service,
        user_configuration,
    )
    variables = subset_request.variables or [
        variable.short_name for variable in service.variables
    ]
    return subset_and_return_dataframe(
        minimum_latitude=subset_request.minimum_y,
        maximum_latitude=subset_request.maximum_y,
        minimum_longitude=subset_request.minimum_x,
        maximum_longitude=subset_request.maximum_x,
        minimum_elevation=(
            -subset_request.minimum_depth
            if subset_request.minimum_depth
            else None
        ),
        maximum_elevation=(
            -subset_request.maximum_depth
            if subset_request.maximum_depth
            else None
        ),
        minimum_time=(
            subset_request.start_datetime.timestamp()
            if subset_request.start_datetime
            else None
        ),
        maximum_time=(
            subset_request.end_datetime.timestamp()
            if subset_request.end_datetime
            else None
        ),
        variables=variables,
        entities=subset_request.platform_ids or [],
        vertical_axis=subset_request.vertical_axis,
        url_metadata=metadata_url,
        user_configuration=user_configuration,
        disable_progress_bar=disable_progress_bar,
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


def _check_plaform_ids(
    platform_ids: list[str],
    metadata_url: str,
    retrieval_service: CopernicusMarineService,
    user_configuration: UserConfiguration,
) -> None:
    if platform_ids:
        platforms_names = get_platforms_names(metadata_url, user_configuration)
        if not platforms_names:
            raise NotEnoughPlatformMetadata()
        for platform_id in platform_ids:
            if platform_id not in platforms_names:
                raise WrongPlatformID(
                    platform_id, retrieval_service.platforms_metadata
                )
