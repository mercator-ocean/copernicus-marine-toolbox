import pathlib
from datetime import datetime
from typing import List, Optional, Union

from copernicusmarine.core_functions.deprecated import deprecated_python_option
from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
)
from copernicusmarine.core_functions.models import (
    DEFAULT_FILE_FORMAT,
    DEFAULT_SUBSET_METHOD,
    FileFormat,
    SubsetMethod,
)
from copernicusmarine.core_functions.subset import subset_function
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.python_interface.utils import homogenize_datetime


@deprecated_python_option(**DEPRECATED_OPTIONS.dict_old_names_to_new_names)
@log_exception_and_exit
def subset(
    dataset_url: Optional[str] = None,
    dataset_id: Optional[str] = None,
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
    vertical_dimension_as_originally_produced: bool = True,
    start_datetime: Optional[Union[datetime, str]] = None,
    end_datetime: Optional[Union[datetime, str]] = None,
    subset_method: SubsetMethod = DEFAULT_SUBSET_METHOD,
    output_filename: Optional[str] = None,
    file_format: FileFormat = DEFAULT_FILE_FORMAT,
    service: Optional[str] = None,
    request_file: Optional[Union[pathlib.Path, str]] = None,
    output_directory: Optional[Union[pathlib.Path, str]] = None,
    credentials_file: Optional[Union[pathlib.Path, str]] = None,
    motu_api_request: Optional[str] = None,
    force_download: bool = False,
    overwrite_output_data: bool = False,
    overwrite_metadata_cache: bool = False,
    no_metadata_cache: bool = False,
    disable_progress_bar: bool = False,
    staging: bool = False,
    netcdf_compression_enabled: bool = False,
    netcdf_compression_level: Optional[int] = None,
    netcdf3_compatible: bool = False,
) -> pathlib.Path:
    """
    Extracts a subset of data from a specified dataset using given parameters.

    Args:
        dataset_url (str, optional): URL of the dataset source.
        dataset_id (str, optional): Identifier for the dataset.
        dataset_version (str, optional): Force use of specified data service.
        dataset_part (str, optional): Force use of specified data service.
        username (str, optional): Username for authentication.
        password (str, optional): Password for authentication.
        variables (List[str], optional): List of variable names to extract.
        minimum_longitude (float, optional): Minimum longitude value for spatial subset.
        maximum_longitude (float, optional): Maximum longitude value for spatial subset.
        minimum_latitude (float, optional): Minimum latitude value for spatial subset.
        maximum_latitude (float, optional): Maximum latitude value for spatial subset.
        minimum_depth (float, optional): Minimum depth value for vertical subset.
        maximum_depth (float, optional): Maximum depth value for vertical subset.
        vertical_dimension_as_originally_produced (bool, optional): Use original vertical dimension.
        start_datetime (datetime, optional): Start datetime for temporal subset.
        end_datetime (datetime, optional): End datetime for temporal subset.
        subset_method (str, optional): The subset method ('nearest' or 'strict') when requesting the dataset. If strict, you can only request dimension strictly inside the dataset.
        output_filename (str, optional): Output filename for the subsetted data.
        file_format (str, optional): Extension format for the filename.
        service (str, optional): Force use of specified data service.
        request_file (Union[pathlib.Path, str], optional): Path to request file.
        output_directory (Union[pathlib.Path, str], optional): Directory to save output files.
        credentials_file (Union[pathlib.Path, str], optional): Path to credentials file.
        motu_api_request (str, optional): MOTU API request string.
        force_download (bool, optional): Skip confirmation before download.
        overwrite_output_data (bool, optional): Overwrite existing output data if True.
        overwrite_metadata_cache (bool, optional): Overwrite existing metadata cache if True.
        no_metadata_cache (bool, optional): Disable metadata caching if True.
        netcdf_compression_enabled (bool, optional): Enable compression level 1 to the NetCDF output file. Use 'netcdf_compression_level' option to customize the compression level.
        netcdf_compression_level (int, optional): Specify a compression level to apply on the NetCDF output file. A value of 0 means no compression, and 9 is the highest level of compression available.
        netcdf3_compatible (bool, optional): Enable downloading the dataset in a netCDF 3 compatible format.
    Returns:
        pathlib.Path: Path to the generated subsetted data file.
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
        dataset_url,
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
        vertical_dimension_as_originally_produced,
        start_datetime,
        end_datetime,
        subset_method,
        output_filename,
        file_format,
        service,
        request_file,
        output_directory,
        credentials_file,
        motu_api_request,
        force_download,
        overwrite_output_data,
        overwrite_metadata_cache,
        no_metadata_cache,
        disable_progress_bar,
        staging=staging,
        netcdf_compression_enabled=netcdf_compression_enabled,
        netcdf_compression_level=netcdf_compression_level,
        netcdf3_compatible=netcdf3_compatible,
    )
