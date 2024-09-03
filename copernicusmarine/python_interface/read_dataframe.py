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
    DEFAULT_BOUNDING_BOX_METHOD,
    DEFAULT_SUBSET_METHOD,
    BoundingBoxMethod,
    SubsetMethod,
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
    vertical_dimension_as_originally_produced: bool = True,
    start_datetime: Optional[Union[datetime, str]] = None,
    end_datetime: Optional[Union[datetime, str]] = None,
    bounding_box_method: BoundingBoxMethod = DEFAULT_BOUNDING_BOX_METHOD,
    subset_method: SubsetMethod = DEFAULT_SUBSET_METHOD,
    force_service: Optional[str] = None,
    credentials_file: Optional[Union[pathlib.Path, str]] = None,
) -> pandas.DataFrame:
    """
    Immediately loads a Pandas DataFrame into memory from a specified dataset.

    Unlike “lazy-loading”, the data is loaded as soon as this function is executed, which may be preferable when rapid access to the entire dataset is required, but may require careful memory management.

    :param dataset_id: The identifier of the dataset.
    :type dataset_id: str, optional
    :param dataset_version: Force a specific dataset version.
    :type dataset_version: str, optional
    :param dataset_part: Force a specific dataset part.
    :type dataset_part: str, optional
    :param username: Username for authentication.
    :type username: str, optional
    :param password: Password for authentication.
    :type password: str, optional
    :param variables: List of variable names to load.
    :type variables: List[str], optional
    :param minimum_longitude: Minimum longitude for spatial subset.
    :type minimum_longitude: float, optional
    :param maximum_longitude: Maximum longitude for spatial subset.
    :type maximum_longitude: float, optional
    :param minimum_latitude: Minimum latitude for spatial subset.
    :type minimum_latitude: float, optional
    :param maximum_latitude: Maximum latitude for spatial subset.
    :type maximum_latitude: float, optional
    :param minimum_depth: Minimum depth for vertical subset.
    :type minimum_depth: float, optional
    :param maximum_depth: Maximum depth for vertical subset.
    :type maximum_depth: float, optional
    :param vertical_dimension_as_originally_produced: If True, use the vertical dimension as originally produced.
    :type vertical_dimension_as_originally_produced: bool, optional
    :param start_datetime: Start datetime for temporal subset.
    :type start_datetime: datetime, optional
    :param end_datetime: End datetime for temporal subset.
    :type end_datetime: datetime, optional
    :param bounding_box_method: The bounding box method ('nearest', 'inside' or 'outside') when requesting the dataset. Check the documentation for more information.
    :type bounding_box_method: str, optional
    :param subset_method: The subset method ('nearest' or 'strict') when requesting the dataset. If strict, you can only request dimension strictly inside the dataset.
    :type subset_method: str, optional
    :param force_service: Force a specific service for data download.
    :type force_service: str, optional
    :param credentials_file: Path to a credentials file for authentication.
    :type credentials_file: Union[pathlib.Path, str], optional

    :returns: A DataFrame containing the loaded Copernicus Marine data.
    :rtype: pandas.DataFrame
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
        subset_method=subset_method,
        depth_parameters=DepthParameters(
            minimum_depth=minimum_depth,
            maximum_depth=maximum_depth,
            vertical_dimension_as_originally_produced=vertical_dimension_as_originally_produced,  # noqa
        ),
        bounding_box_method=bounding_box_method,
        force_service=force_service,
        credentials_file=credentials_file,
    )
    dataset = load_data_object_from_load_request(
        load_request,
        read_dataframe_from_arco_series,
    )
    return dataset
