import pathlib
from datetime import datetime
from typing import List, Optional, Union

from copernicusmarine.core_functions import decorators, documentation_utils
from copernicusmarine.core_functions.deprecated import deprecated_python_option
from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
)
from copernicusmarine.core_functions.models import (
    DEFAULT_COORDINATES_SELECTION_METHOD,
    DEFAULT_FILE_FORMAT,
    DEFAULT_VERTICAL_DIMENSION_OUTPUT,
    CoordinatesSelectionMethod,
    FileFormat,
    ResponseSubset,
    VerticalDimensionOutput,
)
from copernicusmarine.core_functions.subset import subset_function
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.python_interface.utils import homogenize_datetime


@deprecated_python_option(**DEPRECATED_OPTIONS.dict_old_names_to_new_names)
@log_exception_and_exit
@decorators.docstring_parameter(documentation_utils.SUBSET)
def subset(
    dataset_id: Optional[str],
    dataset_version: Optional[str] = None,
    dataset_part: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    variables: Optional[List[str]] = None,
    minimum_longitude: Optional[float] = None,
    maximum_longitude: Optional[float] = None,
    minimum_latitude: Optional[float] = None,
    maximum_latitude: Optional[float] = None,
    minimum_depth: Optional[float] = None,
    maximum_depth: Optional[float] = None,
    vertical_dimension_output: VerticalDimensionOutput = DEFAULT_VERTICAL_DIMENSION_OUTPUT,  # noqa
    start_datetime: Optional[Union[datetime, str]] = None,
    end_datetime: Optional[Union[datetime, str]] = None,
    coordinates_selection_method: CoordinatesSelectionMethod = (
        DEFAULT_COORDINATES_SELECTION_METHOD
    ),
    output_filename: Optional[str] = None,
    file_format: FileFormat = DEFAULT_FILE_FORMAT,
    service: Optional[str] = None,
    request_file: Optional[Union[pathlib.Path, str]] = None,
    output_directory: Optional[Union[pathlib.Path, str]] = None,
    credentials_file: Optional[Union[pathlib.Path, str]] = None,
    motu_api_request: Optional[str] = None,
    force_download: bool = False,
    overwrite_output_data: bool = False,
    dry_run: bool = False,
    disable_progress_bar: bool = False,
    staging: bool = False,
    netcdf_compression_enabled: bool = False,
    netcdf_compression_level: Optional[int] = None,
    netcdf3_compatible: bool = False,
) -> ResponseSubset:
    """
    {SUBSET_DESCRIPTION_HELP}

    Parameters
    ----------
    dataset_id : str, optional
        {DATASET_ID_HELP}
    dataset_version : str, optional
        {DATASET_VERSION_HELP}
    dataset_part : str, optional
        {DATASET_PART_HELP}
    username : str, optional
        {USERNAME_HELP}
    password : str, optional
        {PASSWORD_HELP}
    output_directory : Union[pathlib.Path, str], optional
        {OUTPUT_DIRECTORY_HELP}
    credentials_file : Union[pathlib.Path, str], optional
        {CREDENTIALS_FILE_HELP}
    force_download : bool, optional
        {FORCE_DOWNLOAD_HELP}
    overwrite_output_data : bool, optional
        {OVERWRITE_OUTPUT_DATA_HELP}
    request_file : Union[pathlib.Path, str], optional
        {REQUEST_FILE_HELP}
    service : str, optional
        {SERVICE_HELP}
    variables : List[str], optional
        {VARIABLE_HELP}
    minimum_longitude : float, optional
        {MINIMUM_LONGITUDE_HELP}
    maximum_longitude : float, optional
        {MAXIMUM_LONGITUDE_HELP}
    minimum_latitude : float, optional
        {MINIMUM_LATITUDE_HELP}
    maximum_latitude : float, optional
        {MAXIMUM_LATITUDE_HELP}
    minimum_depth : float, optional
        {MINIMUM_DEPTH_HELP}
    maximum_depth : float, optional
        {MAXIMUM_DEPTH_HELP}
    vertical_dimension_output : str, optional
        {VERTICAL_DIMENSION_OUTPUT_HELP}
    start_datetime : datetime, optional
        {START_DATETIME_HELP}
    end_datetime : datetime, optional
        {END_DATETIME_HELP}
    coordinates_selection_method : str, optional
        {COORDINATES_SELECTION_METHOD_HELP}
    output_filename : str, optional
        {OUTPUT_FILENAME_HELP}
    file_format : str, optional
        {FILE_FORMAT_HELP}
    motu_api_request : str, optional
        {MOTU_API_REQUEST_HELP}
    dry_run : bool, optional
        {DRY_RUN_HELP}
    netcdf_compression_enabled : bool, optional
        {NETCDF_COMPRESSION_ENABLED_HELP}
    netcdf_compression_level : int, optional
        {NETCDF_COMPRESSION_LEVEL_HELP}
    netcdf3_compatible : bool, optional
        {NETCDF_COMPATIBLE_HELP}

    Returns
    -------
    {SUBSET_RESPONSE_HELP}
    """  # noqa
    request_file = pathlib.Path(request_file) if request_file else None
    output_directory = (
        pathlib.Path(output_directory) if output_directory else None
    )
    credentials_file = (
        pathlib.Path(credentials_file) if credentials_file else None
    )

    start_datetime = homogenize_datetime(start_datetime)
    end_datetime = homogenize_datetime(end_datetime)

    return subset_function(
        dataset_id,
        dataset_version,
        dataset_part,
        username,
        password,
        variables,
        minimum_longitude,
        maximum_longitude,
        minimum_latitude,
        maximum_latitude,
        minimum_depth,
        maximum_depth,
        vertical_dimension_output,
        start_datetime,
        end_datetime,
        coordinates_selection_method,
        output_filename,
        file_format,
        service,
        request_file,
        output_directory,
        credentials_file,
        motu_api_request,
        force_download,
        overwrite_output_data,
        dry_run,
        disable_progress_bar,
        staging=staging,
        netcdf_compression_enabled=netcdf_compression_enabled,
        netcdf_compression_level=netcdf_compression_level,
        netcdf3_compatible=netcdf3_compatible,
    )
