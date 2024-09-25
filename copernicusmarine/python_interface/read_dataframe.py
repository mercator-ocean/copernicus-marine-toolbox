import pathlib
from datetime import datetime
from typing import List, Optional, Union

import pandas

from copernicusmarine.catalogue_parser.request_structure import LoadRequest
from copernicusmarine.core_functions.deprecated import (
    deprecated_python_option,
    log_deprecated_message,
)
from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
)
from copernicusmarine.core_functions.models import (
    DEFAULT_COORDINATES_SELECTION_METHOD,
    DEFAULT_VERTICAL_DIMENSION_OUTPUT,
    CoordinatesSelectionMethod,
    VerticalDimensionOutput,
)
from copernicusmarine.download_functions.download_arco_series import (
    read_dataframe_from_arco_series,
)
from copernicusmarine.download_functions.subset_parameters import (
    DepthParameters,
    GeographicalParameters,
    LatitudeParameters,
    LongitudeParameters,
    TemporalParameters,
)
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.python_interface.load_utils import (
    load_data_object_from_load_request,
)
from copernicusmarine.python_interface.utils import homogenize_datetime


@log_exception_and_exit
def load_pandas_dataframe(*args, **kwargs):
    """
    Deprecated function, use 'read_dataframe' instead.
    """
    log_deprecated_message("load_pandas_dataframe", "read_dataframe")
    return read_dataframe(*args, **kwargs)


@deprecated_python_option(**DEPRECATED_OPTIONS.dict_old_names_to_new_names)
@log_exception_and_exit
def read_dataframe(
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
    minimum_depth: Optional[float] = None,
    maximum_depth: Optional[float] = None,
    vertical_dimension_output: VerticalDimensionOutput = DEFAULT_VERTICAL_DIMENSION_OUTPUT,  # noqa
    start_datetime: Optional[Union[datetime, str]] = None,
    end_datetime: Optional[Union[datetime, str]] = None,
    coordinates_selection_method: CoordinatesSelectionMethod = (
        DEFAULT_COORDINATES_SELECTION_METHOD
    ),
    force_service: Optional[str] = None,
    credentials_file: Optional[Union[pathlib.Path, str]] = None,
) -> pandas.DataFrame:
    """
    Immediately loads a Pandas DataFrame into memory from a specified dataset.

    Unlike "lazy-loading," the data is loaded as soon as this function is executed,
    which may be preferable when rapid access to the entire dataset is required,
    but may require careful memory management.

    Parameters
    ----------
    dataset_id : str, optional
        The identifier of the dataset.
    dataset_version : str, optional
        Force a specific dataset version.
    dataset_part : str, optional
        Force a specific dataset part.
    username : str, optional
        Username for authentication.
    password : str, optional
        Password for authentication.
    variables : List[str], optional
        List of variable names to load.
    minimum_longitude : float, optional
        Minimum longitude for spatial subset.
    maximum_longitude : float, optional
        Maximum longitude for spatial subset.
    minimum_latitude : float, optional
        Minimum latitude for spatial subset.
    maximum_latitude : float, optional
        Maximum latitude for spatial subset.
    minimum_depth : float, optional
        Minimum depth for vertical subset.
    maximum_depth : float, optional
        Maximum depth for vertical subset.
    vertical_dimension_output : str, optional
        Consolidate the vertical dimension (the z-axis) as requested: 'depth' with descending positive values.
        'elevation' with ascending positive values. Default is 'depth'.
    start_datetime : datetime, optional
        Start datetime for temporal subset.
    end_datetime : datetime, optional
        End datetime for temporal subset.
    coordinates_selection_method : str, optional
        The method in which the coordinates will be retrieved.If 'strict', the retrieved selection will be inside the requested interval. If 'strict', the retrieved selection will be inside the requested interval and an error will raise if there doesn't exist the values. If 'nearest', the returned interval extremes will be the closest to what has been asked for. A warning will be displayed if outside of bounds. If 'outisde', the extremes will be taken to contain all the requested interval. A warning will also be displayed if the subset is outside of the dataset bounds.
    force_service : str, optional
        Force a specific service for data download.
    credentials_file : Union[pathlib.Path, str], optional
        Path to a credentials file for authentication.

    Returns
    -------
    pandas.DataFrame
        A DataFrame containing the loaded Copernicus Marine data.
    """  # noqa

    start_datetime = homogenize_datetime(start_datetime)
    end_datetime = homogenize_datetime(end_datetime)
    credentials_file = (
        pathlib.Path(credentials_file) if credentials_file else None
    )
    load_request = LoadRequest(
        dataset_id=dataset_id,
        force_dataset_version=dataset_version,
        force_dataset_part=dataset_part,
        username=username,
        password=password,
        variables=variables,
        geographical_parameters=GeographicalParameters(
            latitude_parameters=LatitudeParameters(
                minimum_latitude=minimum_latitude,
                maximum_latitude=maximum_latitude,
            ),
            longitude_parameters=LongitudeParameters(
                minimum_longitude=minimum_longitude,
                maximum_longitude=maximum_longitude,
            ),
        ),
        temporal_parameters=TemporalParameters(
            start_datetime=start_datetime,
            end_datetime=end_datetime,
        ),
        coordinates_selection_method=coordinates_selection_method,
        depth_parameters=DepthParameters(
            minimum_depth=minimum_depth,
            maximum_depth=maximum_depth,
            vertical_dimension_output=vertical_dimension_output,
        ),
        force_service=force_service,
        credentials_file=credentials_file,
    )
    dataset = load_data_object_from_load_request(
        load_request,
        read_dataframe_from_arco_series,
    )
    return dataset
