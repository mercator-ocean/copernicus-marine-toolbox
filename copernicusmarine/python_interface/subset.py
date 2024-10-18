import pathlib
from datetime import datetime
from typing import List, Optional, Union

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
from copernicusmarine.core_functions.utils import deprecated_python_option
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.python_interface.utils import homogenize_datetime


@deprecated_python_option(**DEPRECATED_OPTIONS.dict_old_names_to_new_names)
@log_exception_and_exit
def subset(
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
    netcdf_compression_level: int = 0,
    netcdf3_compatible: bool = False,
) -> ResponseSubset:
    """
    Extracts a subset of data from a specified dataset using given parameters."

    The datasetID is required and can be found via the ``describe`` command.

    Parameters
    ----------
    dataset_id : str, optional
        The datasetID, required either as an argument or in the request_file option.
    dataset_version : str, optional
        Force the selection of a specific dataset version.
    dataset_part : str, optional
        Force the selection of a specific dataset part.
    username : str, optional
        The username for authentication. See also :func:`~copernicusmarine.login`
    password : str, optional
        The password for authentication. See also :func:`~copernicusmarine.login`
    output_directory : Union[pathlib.Path, str], optional
        The destination folder for the downloaded files. Default is the current directory.
    credentials_file : Union[pathlib.Path, str], optional
        Path to a credentials file if not in its default directory (``$HOME/.copernicusmarine``). Accepts .copernicusmarine-credentials / .netrc or _netrc / motuclient-python.ini files.
    force_download : bool, optional
        Flag to skip confirmation before download.
    overwrite_output_data : bool, optional
        If specified and if the file already exists on destination, then it will be overwritten instead of creating new one with unique index.
    request_file : Union[pathlib.Path, str], optional
        Option to pass a file containing the arguments. For more information please refer to the documentation or use option ``--create-template`` from the command line interface for an example template.
    service : str, optional
        Force download through one of the available services using the service name among ['arco-geo-series', 'arco-time-series', 'omi-arco', 'static-arco'] or its short name among ['arco-geo-series', 'arco-time-series', 'omi-arco', 'static-arco'].
    variables : List[str], optional
        List of variable names to extract.
    minimum_longitude : float, optional
        Minimum longitude for the subset. The value will be transposed to the interval [-180; 360[.
    maximum_longitude : float, optional
        Maximum longitude for the subset. The value will be transposed to the interval [-180; 360[.
    minimum_latitude : float, optional
        Minimum latitude for the subset. Requires a float from -90 degrees to +90.
    maximum_latitude : float, optional
        Maximum latitude for the subset. Requires a float from -90 degrees to +90.
    minimum_depth : float, optional
        Minimum depth for the subset. Requires a positive float (or 0).
    maximum_depth : float, optional
        Maximum depth for the subset. Requires a positive float (or 0).
    vertical_dimension_output : str, optional
        Consolidate the vertical dimension (the z-axis) as requested: depth with descending positive values, elevation with ascending positive values. Default is depth.
    start_datetime : Union[datetime, str], optional
        The start datetime of the temporal subset. Supports common format parsed by pendulum (https://pendulum.eustace.io/docs/#parsing).
    end_datetime : Union[datetime, str], optional
        The end datetime of the temporal subset. Supports common format parsed by pendulum (https://pendulum.eustace.io/docs/#parsing).
    coordinates_selection_method : str, optional
        If ``inside``, the selection retrieved will be inside the requested range. If ``strict-inside``, the selection retrieved will be inside the requested range, and an error will be raised if the values don't exist. If ``nearest``, the extremes closest to the requested values will be returned. If ``outside``, the extremes will be taken to contain all the requested interval. The methods ``inside``, ``nearest`` and ``outside`` will display a warning if the request is out of bounds.
    output_filename : str, optional
        Save the downloaded data with the given file name (under the output directory).
    file_format : str, optional
        Format of the downloaded dataset. Default to NetCDF '.nc'.
    motu_api_request : str, optional
        Option to pass a complete MOTU API request as a string. Caution, user has to replace double quotes “ with single quotes ' in the request.
    dry_run : bool, optional
        If True, runs query without downloading data.
    netcdf_compression_level : int, optional
        Specify a compression level to apply on the NetCDF output file. A value of 0 means no compression, and 9 is the highest level of compression available.
    netcdf3_compatible : bool, optional
        Enable downloading the dataset in a netCDF3 compatible format.

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
        netcdf_compression_level=netcdf_compression_level,
        netcdf3_compatible=netcdf3_compatible,
    )
