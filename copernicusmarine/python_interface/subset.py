import pathlib
from datetime import datetime
from typing import List, Optional, Union

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
    Extracts a subset of data from a specified dataset using given parameters.

    Parameters
    ----------
    dataset_id : str, optional
        The unique identifier of the dataset.
    dataset_version : str, optional
        Force the use of a specific dataset version.
    dataset_part : str, optional
        Force the use of a specific dataset part.
    username : str, optional
        The username for authentication. See also :func:`~copernicusmarine.login`.
    password : str, optional
        The password for authentication. See also :func:`~copernicusmarine.login`.
    output_directory : Union[pathlib.Path, str], optional
        The directory where downloaded files will be saved.
    credentials_file : Union[pathlib.Path, str], optional
        Path to a file containing authentication credentials.
    force_download : bool, optional
        Skip confirmation before download.
    overwrite_output_data : bool, optional
        If True, overwrite existing output files.
    request_file : Union[pathlib.Path, str], optional
        Path to a file containing request parameters. For more information, please refer to the README.
    service : str, optional
        Force the use of a specific service.
    variables : List[str], optional
        List of variable names to extract.
    minimum_longitude : float, optional
        Minimum longitude value for spatial subset.
    maximum_longitude : float, optional
        Maximum longitude value for spatial subset.
    minimum_latitude : float, optional
        Minimum latitude value for spatial subset.
    maximum_latitude : float, optional
        Maximum latitude value for spatial subset.
    minimum_depth : float, optional
        Minimum depth value for vertical subset.
    maximum_depth : float, optional
        Maximum depth value for vertical subset.
    vertical_dimension_output : str, optional
        Consolidate the vertical dimension (the z-axis) as requested: 'depth' with descending positive values.
        'elevation' with ascending positive values. Default is 'depth'.
    start_datetime : datetime, optional
        Start datetime for temporal subset.
    end_datetime : datetime, optional
        End datetime for temporal subset.
    coordinates_selection_method : str, optional
        The method in which the coordinates will be retrieved. If 'strict', the retrieved selection will be inside the requested interval. If 'strict', the retrieved selection will be inside the requested interval and an error will raise if there doesn't exist the values. If 'nearest', the returned interval extremes will be the closest to what has been asked for. A warning will be displayed if outside of bounds. If 'outisde', the extremes will be taken to contain all the requested interval. A warning will also be displayed if the subset is outside of the dataset bounds.
    output_filename : str, optional
        Output filename for the subsetted data.
    file_format : str, optional
        Extension format for the filename.
    motu_api_request : str, optional
        MOTU API request string.
    dry_run : bool, optional
        If True, runs query without downloading data.
    netcdf_compression_enabled : bool, optional
        Enable compression level 1 to the NetCDF output file. Use 'netcdf_compression_level' option to customize the
        compression level.
    netcdf_compression_level : int, optional
        Specify a compression level to apply on the NetCDF output file. A value of 0 means no compression, and 9 is the
        highest level of compression available.
    netcdf3_compatible : bool, optional
        Enable downloading the dataset in a netCDF 3 compatible format.

    Returns
    -------
    ResponseSubset
        A description of the downloaded data and its destination.
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
