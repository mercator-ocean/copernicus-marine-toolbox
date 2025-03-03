from arcosparse import subset_and_save

# TODO: update this when available in main
from arcosparse.models import (
    RequestedCoordinate,
    UserConfiguration,
    UserRequest,
)

from copernicusmarine.core_functions.environment_variables import (
    COPERNICUSMARINE_DISABLE_SSL_CONTEXT,
    COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH,
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
)


# TODO: do the case where we want to return a pandas dataframe
# TODO: should we support necdf?
# https://stackoverflow.com/questions/46476920/xarray-writing-to-netcdf-from-pandas-dimension-issue # noqa
def download_sparse(
    username: str,
    subset_request: SubsetRequest,
    disable_progress_bar,
    metadata_url,
) -> ResponseSubset:
    user_request = UserRequest(
        time=RequestedCoordinate(
            minimum=(
                subset_request.start_datetime.timestamp()
                if subset_request.start_datetime
                else None
            ),
            maximum=(
                subset_request.end_datetime.timestamp()
                if subset_request.end_datetime
                else None
            ),
            coordinate_id="time",
        ),
        elevation=RequestedCoordinate(
            minimum=(
                -subset_request.minimum_depth
                if subset_request.minimum_depth
                else None
            ),
            maximum=(
                -subset_request.maximum_depth
                if subset_request.maximum_depth
                else None
            ),
            coordinate_id="depth",
        ),
        latitude=RequestedCoordinate(
            minimum=subset_request.minimum_latitude,
            maximum=subset_request.maximum_latitude,
            coordinate_id="latitude",
        ),
        longitude=RequestedCoordinate(
            minimum=subset_request.minimum_longitude,
            maximum=subset_request.maximum_longitude,
            coordinate_id="longitude",
        ),
        # TODO: add support for platform_ids
        platform_ids=[],
        variables=subset_request.variables or [],
    )

    user_configuration = UserConfiguration(
        disable_ssl=COPERNICUSMARINE_DISABLE_SSL_CONTEXT == "True",
        trust_env=TRUST_ENV,
        ssl_certificate_path=COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH,
        extra_params=construct_query_params_for_marine_data_store_monitoring(
            username
        ),
    )
    # TODO: handle the outputs path, skip existing etc
    _ = subset_and_save(
        url_metadata=metadata_url,
        request=user_request,
        user_configuration=user_configuration,
        output_path=subset_request.output_directory / "sparse_data.parquet",
        disable_progress_bar=disable_progress_bar,
    )
    return ResponseSubset(
        file_path=subset_request.output_directory / "sparse_data.parquet",
        output_directory=subset_request.output_directory,
        filename="sparse_data.parquet",
        file_size=0,
        data_transfer_size=0,
        variables=subset_request.variables or [],
        # TODO: handle thoses extents
        coordinates_extent=[],
        status=StatusCode.SUCCESS,
        message=StatusMessage.SUCCESS,
        file_status=FileStatus.DOWNLOADED,
    )
