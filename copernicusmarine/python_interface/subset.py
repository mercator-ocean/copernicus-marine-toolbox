import pathlib
from datetime import datetime
from typing import List, Optional, Union

import pandas as pd

from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
    deprecated_python_option,
)
from copernicusmarine.core_functions.exceptions import (
    MutuallyExclusiveArguments,
)
from copernicusmarine.core_functions.models import (
    DEFAULT_COORDINATES_SELECTION_METHOD,
    DEFAULT_FILE_FORMAT,
    DEFAULT_VERTICAL_AXIS,
    CoordinatesSelectionMethod,
    FileFormat,
    ResponseSubset,
    VerticalAxis,
)
from copernicusmarine.core_functions.subset import subset_function
from copernicusmarine.core_functions.utils import get_geographical_inputs
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.python_interface.utils import homogenize_datetime


@deprecated_python_option(DEPRECATED_OPTIONS)
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
    vertical_axis: VerticalAxis = DEFAULT_VERTICAL_AXIS,  # noqa
    start_datetime: Optional[Union[datetime, pd.Timestamp, str]] = None,
    end_datetime: Optional[Union[datetime, pd.Timestamp, str]] = None,
    minimum_x: Optional[float] = None,
    maximum_x: Optional[float] = None,
    minimum_y: Optional[float] = None,
    maximum_y: Optional[float] = None,
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
    overwrite: bool = False,
    skip_existing: bool = False,
    dry_run: bool = False,
    disable_progress_bar: bool = False,
    staging: bool = False,
    netcdf_compression_level: int = 0,
    netcdf3_compatible: bool = False,
    chunk_size_limit: int = -1,
    raise_if_updating: bool = False,
    platform_ids: Optional[List[str]] = None,
) -> ResponseSubset:
    """
    Extract a subset of data from a specified dataset using given parameters.

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
        If not set, search for environment variable COPERNICUSMARINE_SERVICE_USERNAME, then search for a credentials file, else ask for user input. See also :func:`~copernicusmarine.login`
    password : str, optional
        If not set, search for environment variable COPERNICUSMARINE_SERVICE_PASSWORD, then search for a credentials file, else ask for user input. See also :func:`~copernicusmarine.login`
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
    minimum_x : float, optional
        Minimum x-axis value for the subset. The units are considered in length (m, 100km...).
    maximum_x : float, optional
        Maximum x-axis value for the subset. The units are considered in length (m, 100km...).
    minimum_y : float, optional
        Minimum y-axis value for the subset. The units are considered in length (m, 100km...).
    maximum_y : float, optional
        Maximum y-axis value for the subset. The units are considered in length (m, 100km...).
    minimum_depth : float, optional
        Minimum depth for the subset. Requires a positive float (or 0).
    maximum_depth : float, optional
        Maximum depth for the subset. Requires a positive float (or 0).
    vertical_axis : str, optional
        Consolidate the vertical dimension (the z-axis) as requested: depth with descending positive values, elevation with ascending positive values. Default is depth.
    start_datetime : Union[datetime, str], optional
        The start datetime of the temporal subset. Supports common format parsed by dateutil (https://dateutil.readthedocs.io/en/stable/parser.html).
    end_datetime : Union[datetime, str], optional
        The end datetime of the temporal subset. Supports common format parsed by dateutil (https://dateutil.readthedocs.io/en/stable/parser.html).
    coordinates_selection_method : str, optional
        If ``inside``, the selection retrieved will be inside the requested range. If ``strict-inside``, the selection retrieved will be inside the requested range, and an error will be raised if the values don't exist. If ``nearest``, the extremes closest to the requested values will be returned. If ``outside``, the extremes will be taken to contain all the requested interval. The methods ``inside``, ``nearest`` and ``outside`` will display a warning if the request is out of bounds.
    output_directory : Union[pathlib.Path, str], optional
        The destination folder for the downloaded files. Default is the current directory.
    credentials_file : Union[pathlib.Path, str], optional
        Path to a credentials file if not in its default directory (``$HOME/.copernicusmarine``). Accepts .copernicusmarine-credentials / .netrc or _netrc / motuclient-python.ini files.
    output_filename : str, optional
        Save the downloaded data with the given file name (under the output directory).
    file_format : str, optional
        Format of the downloaded dataset. Default to NetCDF '.nc'.
    overwrite : bool, optional
        If specified and if the file already exists on destination, then it will be overwritten. By default, the toolbox creates a new file with a new index (eg 'filename_(1).nc').
    skip_existing : bool, optional
        If the files already exists where it would be downloaded, then the download is skipped for this file. By default, the toolbox creates a new file with a new index (eg 'filename_(1).nc').
    service : str, optional
        Force download through one of the available services using the service name among ['arco-geo-series', 'arco-time-series', 'omi-arco', 'static-arco', 'arco-platform-series'] or its short name among ['geoseries', 'timeseries', 'omi-arco', 'static-arco', 'platformseries'].
    request_file : Union[pathlib.Path, str], optional
        Option to pass a file containing the arguments. For more information please refer to the documentation or use option ``--create-template`` from the command line interface for an example template.
    motu_api_request : str, optional
        Option to pass a complete MOTU API request as a string. Caution, user has to replace double quotes " with single quotes ' in the request.
    dry_run : bool, optional
        If True, runs query without downloading data.
    netcdf_compression_level : int, optional
        Specify a compression level to apply on the NetCDF output file. A value of 0 means no compression, and 9 is the highest level of compression available.
    netcdf3_compatible : bool, optional
        Enable downloading the dataset in a netCDF3 compatible format.
    chunk_size_limit : int, default -1
        Limit the size of the chunks in the dask array. Default is set to -1 which behaves similarly to 'chunks=auto' from ``xarray``. Positive integer values and '-1' are accepted. This is an experimental feature.
    raise_if_updating : bool, default False
        If set, raises a :class:`copernicusmarine.DatasetUpdating` error if the dataset is being updated and the subset interval requested overpasses the updating start date of the dataset. Otherwise, a simple warning is displayed.
    platform_ids : List[str], optional
        List of platform IDs to extract. Only available for platform chunked datasets.

    Returns
    -------
    ResponseSubset
        A description of the downloaded data and its destination.

    """  # noqa
    if overwrite:
        if skip_existing:
            raise MutuallyExclusiveArguments("overwrite", "skip_existing")

    request_file = pathlib.Path(request_file) if request_file else None
    output_directory = (
        pathlib.Path(output_directory) if output_directory else None
    )
    credentials_file = (
        pathlib.Path(credentials_file) if credentials_file else None
    )

    if variables is not None:
        _check_type(variables, list, "variables")
    if platform_ids is not None:
        _check_type(platform_ids, list, "platform_ids")

    start_datetime = homogenize_datetime(start_datetime)
    end_datetime = homogenize_datetime(end_datetime)

    (
        minimum_x_axis,
        maximum_x_axis,
        minimum_y_axis,
        maximum_y_axis,
    ) = get_geographical_inputs(
        minimum_longitude,
        maximum_longitude,
        minimum_latitude,
        maximum_latitude,
        minimum_x,
        maximum_x,
        minimum_y,
        maximum_y,
        dataset_part,
    )

    return subset_function(
        dataset_id,
        dataset_version,
        dataset_part,
        username,
        password,
        variables,
        minimum_x_axis,
        maximum_x_axis,
        minimum_y_axis,
        maximum_y_axis,
        minimum_depth,
        maximum_depth,
        vertical_axis,
        start_datetime,
        end_datetime,
        platform_ids,
        coordinates_selection_method,
        output_filename,
        file_format,
        service,
        request_file,
        output_directory,
        credentials_file,
        motu_api_request,
        overwrite,
        skip_existing,
        dry_run,
        disable_progress_bar,
        staging,
        netcdf_compression_level,
        netcdf3_compatible,
        chunk_size_limit,
        raise_if_updating,
    )


def _check_type(value, expected_type: type, name: str):
    if not isinstance(value, expected_type):
        raise TypeError(f"{name} must be of type {expected_type.__name__}")
