import pathlib
from datetime import datetime
from typing import List, Optional, Union

import pandas as pd
import xarray

from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
    deprecated_python_option,
)
from copernicusmarine.core_functions.models import (
    DEFAULT_COORDINATES_SELECTION_METHOD,
    DEFAULT_VERTICAL_AXIS,
    CommandType,
    CoordinatesSelectionMethod,
    VerticalAxis,
)
from copernicusmarine.core_functions.request_structure import LoadRequest
from copernicusmarine.core_functions.utils import get_geographical_inputs
from copernicusmarine.download_functions.download_zarr import (
    open_dataset_from_arco_series,
)
from copernicusmarine.download_functions.subset_parameters import (
    DepthParameters,
    GeographicalParameters,
    TemporalParameters,
    XParameters,
    YParameters,
)
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.python_interface.load_utils import (
    load_data_object_from_load_request,
)
from copernicusmarine.python_interface.utils import homogenize_datetime


@deprecated_python_option(DEPRECATED_OPTIONS)
@log_exception_and_exit
def open_dataset(
    dataset_id: str,
    dataset_version: Optional[str] = None,
    dataset_part: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    variables: Optional[List[str]] = None,
    minimum_longitude: Optional[float] = None,
    maximum_longitude: Optional[float] = None,
    minimum_latitude: Optional[float] = None,
    maximum_latitude: Optional[float] = None,
    maximum_x: Optional[float] = None,
    minimum_x: Optional[float] = None,
    maximum_y: Optional[float] = None,
    minimum_y: Optional[float] = None,
    minimum_depth: Optional[float] = None,
    maximum_depth: Optional[float] = None,
    vertical_axis: VerticalAxis = DEFAULT_VERTICAL_AXIS,
    start_datetime: Optional[Union[datetime, pd.Timestamp, str]] = None,
    end_datetime: Optional[Union[datetime, pd.Timestamp, str]] = None,
    coordinates_selection_method: CoordinatesSelectionMethod = (
        DEFAULT_COORDINATES_SELECTION_METHOD
    ),
    service: Optional[str] = None,
    credentials_file: Optional[Union[pathlib.Path, str]] = None,
    chunk_size_limit: int = -1,
) -> xarray.Dataset:
    """
    Load an xarray dataset using 'lazy-loading' mode from a Copernicus Marine data source.

    This means that data is only loaded into memory when a computation is called, optimizing RAM usage by avoiding immediate loading.
    It supports various parameters for customization, such as specifying geographical bounds, temporal range, depth range, and more.


    Parameters
    ----------
    dataset_id : str
        The datasetID, required.
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
    service : str, optional
        Force download through one of the available services using the service name among ['arco-geo-series', 'arco-time-series', 'omi-arco', 'static-arco', 'arco-platform-series'] or its short name among ['geoseries', 'timeseries', 'omi-arco', 'static-arco', 'platformseries'].
    credentials_file : Union[pathlib.Path, str], optional
        Path to a credentials file if not in its default directory (``$HOME/.copernicusmarine``). Accepts .copernicusmarine-credentials / .netrc or _netrc / motuclient-python.ini files.
    chunk_size_limit : int, default -1
        Limit the size of the chunks in the dask array. Default is set to -1 which behaves similarly to 'chunks=auto' from ``xarray``. Positive integer values and '-1' are accepted. This is an experimental feature.

    Returns
    -------
    xarray.Dataset
        The loaded xarray dataset.
    """  # noqa
    start_datetime = homogenize_datetime(start_datetime)
    end_datetime = homogenize_datetime(end_datetime)
    credentials_file = (
        pathlib.Path(credentials_file) if credentials_file else None
    )

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

    geographicalparameters = GeographicalParameters(
        y_axis_parameters=YParameters(
            minimum_y=minimum_y_axis,
            maximum_y=maximum_y_axis,
            coordinate_id=(
                "y" if dataset_part == "originalGrid" else "latitude"
            ),
        ),
        x_axis_parameters=XParameters(
            minimum_x=minimum_x_axis,
            maximum_x=maximum_x_axis,
            coordinate_id=(
                "x" if dataset_part == "originalGrid" else "longitude"
            ),
        ),
        projection=(
            "originalGrid" if dataset_part == "originalGrid" else "lonlat"
        ),
    )

    load_request = LoadRequest(
        dataset_id=dataset_id,
        force_dataset_version=dataset_version,
        force_dataset_part=dataset_part,
        username=username,
        password=password,
        variables=variables,
        geographical_parameters=geographicalparameters,
        temporal_parameters=TemporalParameters(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        ),
        depth_parameters=DepthParameters(
            minimum_depth=minimum_depth,
            maximum_depth=maximum_depth,
            vertical_axis=vertical_axis,
        ),
        coordinates_selection_method=coordinates_selection_method,
        force_service=service,
        credentials_file=credentials_file,
    )

    dataset = load_data_object_from_load_request(
        load_request,
        open_dataset_from_arco_series,
        chunks_factor_size_limit=chunk_size_limit,
        command_type=CommandType.OPEN_DATASET,
    )
    return dataset
