import pathlib
from datetime import datetime
from typing import List, Optional, Union

import xarray

from copernicusmarine.catalogue_parser.request_structure import LoadRequest
from copernicusmarine.core_functions import decorators, documentation_utils
from copernicusmarine.core_functions.deprecated import deprecated_python_option
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
    open_dataset_from_arco_series,
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


@decorators.docstring_parameter(documentation_utils.SUBSET)
@deprecated_python_option(**DEPRECATED_OPTIONS.dict_old_names_to_new_names)
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
    minimum_depth: Optional[float] = None,
    maximum_depth: Optional[float] = None,
    vertical_dimension_output: VerticalDimensionOutput = DEFAULT_VERTICAL_DIMENSION_OUTPUT,  # noqa
    start_datetime: Optional[Union[datetime, str]] = None,
    end_datetime: Optional[Union[datetime, str]] = None,
    coordinates_selection_method: CoordinatesSelectionMethod = (
        DEFAULT_COORDINATES_SELECTION_METHOD
    ),
    service: Optional[str] = None,
    credentials_file: Optional[Union[pathlib.Path, str]] = None,
) -> xarray.Dataset:
    """
    {OPEN_DESCRIPTION_HELP}

    Parameters
    ----------
    dataset_id : str
        {DATASET_ID_HELP}
    dataset_version : str, optional
        {DATASET_VERSION_HELP}
    dataset_part : str, optional
        {DATASET_PART_HELP}
    username : str, optional
        {USERNAME_HELP}
    password : str, optional
        {PASSWORD_HELP}
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
    coordinates_selection_method : str, optional
        {COORDINATES_SELECTION_METHOD_HELP}
    vertical_dimension_output : str, optional
        {VERTICAL_DIMENSION_OUTPUT_HELP}
    start_datetime : datetime, optional
        {START_DATETIME_HELP}
    end_datetime : datetime, optional
        {END_DATETIME_HELP}
    service : str, optional
        {SERVICE_HELP}
    credentials_file : Union[pathlib.Path, str], optional
        {CREDENTIALS_FILE_HELP}

    Returns
    -------
    {OPEN_RESPONSE_HELP}
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
        depth_parameters=DepthParameters(
            minimum_depth=minimum_depth,
            maximum_depth=maximum_depth,
            vertical_dimension_output=vertical_dimension_output,
        ),
        coordinates_selection_method=coordinates_selection_method,
        force_service=service,
        credentials_file=credentials_file,
    )
    dataset = load_data_object_from_load_request(
        load_request,
        open_dataset_from_arco_series,
    )
    return dataset
