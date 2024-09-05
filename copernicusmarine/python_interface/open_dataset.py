import pathlib
from datetime import datetime
from typing import List, Optional, Union

import xarray

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


@log_exception_and_exit
def load_xarray_dataset(*args, **kwargs):
    """
    Deprecated function, use 'open_dataset' instead.
    """
    log_deprecated_message("load_xarray_dataset", "open_dataset")
    return open_dataset(*args, **kwargs)


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
    vertical_dimension_as_originally_produced: bool = True,
    start_datetime: Optional[Union[datetime, str]] = None,
    end_datetime: Optional[Union[datetime, str]] = None,
    bounding_box_method: BoundingBoxMethod = DEFAULT_BOUNDING_BOX_METHOD,
    subset_method: SubsetMethod = DEFAULT_SUBSET_METHOD,
    service: Optional[str] = None,
    credentials_file: Optional[Union[pathlib.Path, str]] = None,
) -> xarray.Dataset:
    """
    Load an xarray dataset using "lazy-loading" mode from a Copernicus Marine data source using either the ARCO series protocol.

    This means that data is only loaded into memory when a computation is called, optimizing RAM usage by avoiding immediate loading. It supports various parameters for customization, such as specifying geographical bounds, temporal range, depth range, and more.

    :param dataset_id: The ID of the dataset. `dataset_id` is mandatory.
    :type dataset_id: str, optional
    :param dataset_version: Force the use of a specific dataset version.
    :type dataset_version: str, optional
    :param dataset_part: Force the use of a specific dataset part.
    :type dataset_part: str, optional
    :param username: Username for authentication, if required.
    :type username: str, optional
    :param password: Password for authentication, if required.
    :type password: str, optional
    :param variables: List of variable names to be loaded from the dataset.
    :type variables: List[str], optional
    :param minimum_longitude: The minimum longitude for subsetting the data.
    :type minimum_longitude: float, optional
    :param maximum_longitude: The maximum longitude for subsetting the data.
    :type maximum_longitude: float, optional
    :param minimum_latitude: The minimum latitude for subsetting the data.
    :type minimum_latitude: float, optional
    :param maximum_latitude: The maximum latitude for subsetting the data.
    :type maximum_latitude: float, optional
    :param minimum_depth: The minimum depth for subsetting the data.
    :type minimum_depth: float, optional
    :param maximum_depth: The maximum depth for subsetting the data.
    :type maximum_depth: float, optional
    :param bounding_box_method: The bounding box method when requesting the dataset. If 'inside' (by default), it will returned the inside interval. If 'nearest', the limits of the requested interval will be the nearest points of the dataset. If 'outside', it will return all the data such that the requested interval is fully included. Check the documentation for more details.
    :type bounding_box_method: str, optional
    :param subset_method: The subset method ('nearest' or 'strict') when requesting the dataset. If strict, you can only request dimension strictly inside the dataset.
    :type subset_method: str, optional
    :param vertical_dimension_as_originally_produced: If True, use the vertical dimension as originally produced.
    :type vertical_dimension_as_originally_produced: bool, optional
    :param start_datetime: The start datetime for temporal subsetting.
    :type start_datetime: datetime, optional
    :param end_datetime: The end datetime for temporal subsetting.
    :type end_datetime: datetime, optional
    :param service: Force the use of a specific service (ARCO geo series or time series).
    :type service: str, optional
    :param credentials_file: Path to a file containing authentication credentials.
    :type credentials_file: Union[pathlib.Path, str], optional

    :returns: The loaded xarray dataset.
    :rtype: `xarray.Dataset`
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
            vertical_dimension_as_originally_produced=vertical_dimension_as_originally_produced,  # noqa
        ),
        bounding_box_method=bounding_box_method,
        subset_method=subset_method,
        force_service=service,
        credentials_file=credentials_file,
    )
    dataset = load_data_object_from_load_request(
        load_request,
        open_dataset_from_arco_series,
    )
    return dataset
