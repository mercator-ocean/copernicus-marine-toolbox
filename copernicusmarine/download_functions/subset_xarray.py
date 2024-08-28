import logging
import typing
from datetime import datetime
from decimal import Decimal
from typing import List, Literal, Optional, Union

import numpy
import xarray
from pandas import Timestamp

from copernicusmarine.catalogue_parser.catalogue_parser import (
    CopernicusMarineDatasetServiceType,
)
from copernicusmarine.catalogue_parser.request_structure import (
    DatasetTimeAndGeographicalSubset,
)
from copernicusmarine.core_functions import custom_open_zarr
from copernicusmarine.core_functions.exceptions import (
    CoordinatesOutOfDatasetBounds,
    MinimumLongitudeGreaterThanMaximumLongitude,
    VariableDoesNotExistInTheDataset,
)
from copernicusmarine.core_functions.models import SubsetMethod
from copernicusmarine.core_functions.utils import (
    ServiceNotSupported,
    convert_datetime64_to_netcdf_timestamp,
)
from copernicusmarine.download_functions.subset_parameters import (
    DepthParameters,
    GeographicalParameters,
    LatitudeParameters,
    LongitudeParameters,
    TemporalParameters,
)

logger = logging.getLogger("copernicus_marine_root_logger")

COORDINATES_LABEL = {
    "latitude": ["latitude", "nav_lat", "x", "lat"],
    "longitude": ["longitude", "nav_lon", "y", "lon"],
    "time": ["time_counter", "time"],
    "depth": ["depth", "deptht", "elevation"],
}


def _dataset_custom_sel(
    dataset: xarray.Dataset,
    coord_type: Literal["latitude", "longitude", "depth", "time"],
    coord_selection: Union[float, slice, datetime, None],
    method: Union[str, None] = None,
) -> xarray.Dataset:
    for coord_label in COORDINATES_LABEL[coord_type]:
        if coord_label in dataset.sizes:
            tmp_dataset = dataset.sel(
                {coord_label: coord_selection}, method=method
            )
            if tmp_dataset.coords[coord_label].size == 0 or (
                coord_label not in tmp_dataset.sizes
            ):
                target = (
                    coord_selection.start
                    if isinstance(coord_selection, slice)
                    else coord_selection
                )
                nearest_neighbor_value = dataset.sel(
                    {coord_label: target}, method="nearest"
                )[coord_label].values
                dataset = dataset.sel(
                    {
                        coord_label: slice(
                            nearest_neighbor_value, nearest_neighbor_value
                        )
                    }
                )
            else:
                dataset = tmp_dataset
    return dataset


def get_size_of_coordinate_subset(
    dataset: xarray.Dataset,
    coordinate: str,
    minimum: Optional[Union[float, datetime]],
    maximum: Optional[Union[float, datetime]],
) -> int:
    for label in COORDINATES_LABEL[coordinate]:
        if label in dataset.sizes:
            return (
                dataset.coords[coordinate]
                .sel({coordinate: slice(minimum, maximum)}, method=None)
                .coords[coordinate]
                .size
            )
    else:
        raise KeyError(
            f"Could not subset on {coordinate}. "
            "Didn't find an equivalent in the dataset."
        )


def _shift_longitude_dimension(
    dataset: xarray.Dataset,
    minimum_longitude_modulus: float,
):
    window = (
        minimum_longitude_modulus + 180
    )  # compute the degrees needed to move the dataset
    for coord_label in COORDINATES_LABEL["longitude"]:
        if coord_label in dataset.sizes:
            attrs = dataset[coord_label].attrs
            if "valid_min" in attrs:
                attrs["valid_min"] += window
            if "valid_max" in attrs:
                attrs["valid_max"] += window
            dataset = dataset.assign_coords(
                {
                    coord_label: (
                        (dataset[coord_label] + (180 - window)) % 360
                    )
                    - (180 - window)
                }
            ).sortby(coord_label)
            dataset[coord_label].attrs = attrs
    return dataset


def _latitude_subset(
    dataset: xarray.Dataset,
    latitude_parameters: LatitudeParameters,
) -> xarray.Dataset:
    minimum_latitude = latitude_parameters.minimum_latitude
    maximum_latitude = latitude_parameters.maximum_latitude
    if minimum_latitude is not None or maximum_latitude is not None:
        latitude_selection = (
            minimum_latitude
            if minimum_latitude == maximum_latitude
            else slice(minimum_latitude, maximum_latitude)
        )
        latitude_method = (
            "nearest" if minimum_latitude == maximum_latitude else None
        )
        dataset = _dataset_custom_sel(
            dataset, "latitude", latitude_selection, latitude_method
        )

    return dataset


def _longitude_subset(
    dataset: xarray.Dataset,
    longitude_parameters: LongitudeParameters,
) -> xarray.Dataset:
    minimum_longitude = longitude_parameters.minimum_longitude
    maximum_longitude = longitude_parameters.maximum_longitude
    longitude_method = None
    if minimum_longitude is not None or maximum_longitude is not None:
        if minimum_longitude is not None and maximum_longitude is not None:
            if minimum_longitude > maximum_longitude:
                raise MinimumLongitudeGreaterThanMaximumLongitude(
                    "--minimum-longitude option must be smaller "
                    "or equal to --maximum-longitude"
                )
            if maximum_longitude - minimum_longitude >= 360:
                longitude_selection: Union[float, slice, None] = None
            elif minimum_longitude == maximum_longitude:
                longitude_selection = longitude_modulus(minimum_longitude)
                longitude_method = "nearest"
            else:
                minimum_longitude_modulus = longitude_modulus(
                    minimum_longitude
                )
                maximum_longitude_modulus = longitude_modulus(
                    maximum_longitude
                )
                if maximum_longitude_modulus < minimum_longitude_modulus:
                    maximum_longitude_modulus += 360
                    dataset = _shift_longitude_dimension(
                        dataset, minimum_longitude_modulus
                    )
                longitude_selection = slice(
                    minimum_longitude_modulus,
                    maximum_longitude_modulus,
                )
        else:
            longitude_selection = slice(minimum_longitude, maximum_longitude)

        if longitude_selection is not None:
            dataset = _dataset_custom_sel(
                dataset, "longitude", longitude_selection, longitude_method
            )
    return dataset


def _temporal_subset(
    dataset: xarray.Dataset,
    temporal_parameters: TemporalParameters,
) -> xarray.Dataset:
    start_datetime = temporal_parameters.start_datetime
    end_datetime = temporal_parameters.end_datetime
    if start_datetime is not None or end_datetime is not None:
        temporal_selection = (
            start_datetime
            if start_datetime == end_datetime
            else slice(start_datetime, end_datetime)
        )
        temporal_method = "nearest" if start_datetime == end_datetime else None
        dataset = _dataset_custom_sel(
            dataset, "time", temporal_selection, temporal_method
        )
    return dataset


def _depth_subset(
    dataset: xarray.Dataset,
    depth_parameters: DepthParameters,
) -> xarray.Dataset:
    def convert_elevation_to_depth(dataset: xarray.Dataset):
        if "elevation" in dataset.sizes:
            attrs = dataset["elevation"].attrs
            dataset = dataset.reindex(elevation=dataset.elevation[::-1])
            dataset["elevation"] = dataset.elevation * (-1)
            attrs["positive"] = "down"
            attrs["standard_name"] = "depth"
            attrs["long_name"] = "Depth"
            attrs["units"] = "m"
            dataset = dataset.rename({"elevation": "depth"})
            dataset.depth.attrs = attrs
        return dataset

    def update_elevation_attributes(dataset: xarray.Dataset):
        if "elevation" in dataset.sizes:
            attrs = dataset["elevation"].attrs
            attrs["positive"] = "up"
            attrs["standard_name"] = "elevation"
            attrs["long_name"] = "Elevation"
            attrs["units"] = "m"
            dataset["elevation"].attrs = attrs
        return dataset

    if depth_parameters.vertical_dimension_as_originally_produced:
        dataset = convert_elevation_to_depth(dataset)
    else:
        dataset = update_elevation_attributes(dataset)
    minimum_depth = depth_parameters.minimum_depth
    maximum_depth = depth_parameters.maximum_depth
    if minimum_depth is not None or maximum_depth is not None:
        coords = (
            dataset.coords if isinstance(dataset, xarray.Dataset) else dataset
        )
        if "elevation" in coords:
            minimum_depth = (
                minimum_depth * -1.0 if minimum_depth is not None else None
            )
            maximum_depth = (
                maximum_depth * -1.0 if maximum_depth is not None else None
            )
            minimum_depth, maximum_depth = maximum_depth, minimum_depth

        depth_selection = (
            minimum_depth
            if minimum_depth == maximum_depth
            else slice(minimum_depth, maximum_depth)
        )
        depth_method = "nearest" if minimum_depth == maximum_depth else None
        dataset = _dataset_custom_sel(
            dataset, "depth", depth_selection, depth_method
        )
    return dataset


def _get_variable_name_from_standard_name(
    dataset: xarray.Dataset, standard_name: str
) -> Optional[str]:
    for variable_name in dataset.variables:
        if (
            hasattr(dataset[variable_name], "standard_name")
            and dataset[variable_name].standard_name == standard_name
        ):
            return str(variable_name)
    return None


def _variables_subset(
    dataset: xarray.Dataset, variables: List[str]
) -> xarray.Dataset:
    dataset_variables_filter = []

    for variable in variables:
        if variable in dataset.variables:
            dataset_variables_filter.append(variable)
        else:
            variable_name_from_standard_name = (
                _get_variable_name_from_standard_name(dataset, variable)
            )
            if variable_name_from_standard_name is not None:
                dataset_variables_filter.append(
                    variable_name_from_standard_name
                )
            else:
                raise VariableDoesNotExistInTheDataset(variable)
    return dataset[numpy.array(dataset_variables_filter)]


def _update_dataset_coordinate_valid_minmax_attributes(
    dataset: xarray.Dataset,
) -> xarray.Dataset:
    for coordinate_label in COORDINATES_LABEL:
        for coordinate_alias in COORDINATES_LABEL[coordinate_label]:
            if coordinate_alias in dataset.sizes:
                coord = dataset[coordinate_alias]
                attrs = coord.attrs
                if "time" in coordinate_label:
                    min_time_dimension = coord.values.min()
                    max_time_dimension = coord.values.max()
                    netcdf_unit = coord.encoding["units"]
                    valid_min = convert_datetime64_to_netcdf_timestamp(
                        min_time_dimension, netcdf_unit
                    )
                    valid_max = convert_datetime64_to_netcdf_timestamp(
                        max_time_dimension, netcdf_unit
                    )
                    attrs["valid_min"] = valid_min
                    attrs["valid_max"] = valid_max
                else:
                    attrs["valid_min"] = coord.values.min()
                    attrs["valid_max"] = coord.values.max()

                coord.attrs = attrs

    return dataset


def subset(
    dataset: xarray.Dataset,
    variables: Optional[List[str]],
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
) -> xarray.Dataset:
    if variables:
        dataset = _variables_subset(dataset, variables)

    dataset = _latitude_subset(
        dataset, geographical_parameters.latitude_parameters
    )
    dataset = _longitude_subset(
        dataset, geographical_parameters.longitude_parameters
    )

    dataset = _temporal_subset(dataset, temporal_parameters)

    dataset = _depth_subset(dataset, depth_parameters)

    dataset = _update_dataset_coordinate_valid_minmax_attributes(dataset)

    return dataset


def longitude_modulus(longitude: float) -> float:
    """
    Returns the equivalent longitude in [-180, 180[
    """
    # We are using Decimal to avoid issue with rounding
    modulus = float(Decimal(str(longitude + 180)) % 360)
    # Modulus with python return a negative value if the denominator is negative
    # To counteract that, we add 360 if the result is < 0
    modulus = modulus if modulus >= 0 else modulus + 360
    return modulus - 180


def longitude_modulus_upper_bound(longitude: float) -> float:
    """
    Returns the equivalent longitude in ]-180, 180]
    """
    modulus = longitude_modulus(longitude)
    if modulus == -180:
        return 180.0
    return modulus


def check_dataset_subset_bounds(
    username: str,
    password: str,
    dataset_url: str,
    service_type: CopernicusMarineDatasetServiceType,
    dataset_subset: DatasetTimeAndGeographicalSubset,
    subset_method: SubsetMethod,
    dataset_valid_date: Optional[Union[str, int]],
) -> None:
    if service_type in [
        CopernicusMarineDatasetServiceType.GEOSERIES,
        CopernicusMarineDatasetServiceType.TIMESERIES,
        CopernicusMarineDatasetServiceType.OMI_ARCO,
        CopernicusMarineDatasetServiceType.STATIC_ARCO,
    ]:
        dataset = custom_open_zarr.open_zarr(
            dataset_url, copernicus_marine_username=username
        )
        dataset_coordinates = dataset.coords
    else:
        raise ServiceNotSupported(service_type)
    for coordinate_label in COORDINATES_LABEL["latitude"]:
        if coordinate_label in dataset.sizes:
            latitudes = dataset_coordinates[coordinate_label].values
            user_minimum_coordinate_value = (
                dataset_subset.minimum_latitude
                if dataset_subset.minimum_latitude is not None
                else latitudes.min()
            )
            user_maximum_coordinate_value = (
                dataset_subset.maximum_latitude
                if dataset_subset.maximum_latitude is not None
                else latitudes.max()
            )
            _check_coordinate_overlap(
                dimension="latitude",
                user_minimum_coordinate_value=user_minimum_coordinate_value,
                user_maximum_coordinate_value=user_maximum_coordinate_value,
                dataset_minimum_coordinate_value=latitudes.min(),
                dataset_maximum_coordinate_value=latitudes.max(),
                is_strict=subset_method == "strict",
            )
    for coordinate_label in COORDINATES_LABEL["longitude"]:
        if coordinate_label in dataset.sizes:
            longitudes = dataset_coordinates[coordinate_label].values
            _check_coordinate_overlap(
                dimension="longitude",
                user_minimum_coordinate_value=(
                    longitude_modulus(dataset_subset.minimum_longitude)
                    if dataset_subset.minimum_longitude is not None
                    else longitudes.min()
                ),
                user_maximum_coordinate_value=(
                    longitude_modulus_upper_bound(
                        dataset_subset.maximum_longitude
                    )
                    if dataset_subset.maximum_longitude is not None
                    else longitudes.max()
                ),
                dataset_minimum_coordinate_value=longitudes.min(),
                dataset_maximum_coordinate_value=longitudes.max(),
                is_strict=subset_method == "strict",
            )
    for coordinate_label in COORDINATES_LABEL["time"]:
        if coordinate_label in dataset.sizes:
            times = dataset_coordinates[coordinate_label].values
            if dataset_valid_date:
                times_min = dataset_valid_date
            else:
                times_min = times.min()
            dataset_minimum_coordinate_value = date_to_datetime(times_min)
            dataset_maximum_coordinate_value = date_to_datetime(times.max())
            user_minimum_coordinate_value = (
                dataset_subset.start_datetime
                if dataset_subset.start_datetime is not None
                else dataset_minimum_coordinate_value
            )
            user_maximum_coordinate_value = (
                dataset_subset.end_datetime
                if dataset_subset.end_datetime is not None
                else dataset_maximum_coordinate_value
            )
            _check_coordinate_overlap(
                dimension="time",
                user_minimum_coordinate_value=user_minimum_coordinate_value,
                user_maximum_coordinate_value=user_maximum_coordinate_value,
                dataset_minimum_coordinate_value=dataset_minimum_coordinate_value,
                dataset_maximum_coordinate_value=dataset_maximum_coordinate_value,
                is_strict=subset_method == "strict",
            )


def date_to_datetime(date: Union[str, int]) -> datetime:
    if isinstance(date, int):
        return Timestamp(date * 1e6).to_pydatetime()
    else:
        return Timestamp(date).to_pydatetime().replace(tzinfo=None)


@typing.no_type_check
def _check_coordinate_overlap(
    dimension: str,
    user_minimum_coordinate_value: Union[float, datetime],
    user_maximum_coordinate_value: Union[float, datetime],
    dataset_minimum_coordinate_value: Union[float, datetime],
    dataset_maximum_coordinate_value: Union[float, datetime],
    is_strict: bool,
) -> None:
    message = (
        f"Some or all of your subset selection "
        f"[{user_minimum_coordinate_value}, {user_maximum_coordinate_value}] "
        f"for the {dimension} dimension  exceed the dataset coordinates "
        f"[{dataset_minimum_coordinate_value}, "
        f"{dataset_maximum_coordinate_value}]"
    )
    if dimension == "longitude":
        if dataset_minimum_coordinate_value == -180:
            dataset_maximum_coordinate_value = 180
        if dataset_maximum_coordinate_value == 180:
            dataset_minimum_coordinate_value = -180
    if (
        user_maximum_coordinate_value < dataset_minimum_coordinate_value
        or user_minimum_coordinate_value > dataset_maximum_coordinate_value
    ):
        raise CoordinatesOutOfDatasetBounds(message)
    elif (
        (
            user_minimum_coordinate_value < dataset_minimum_coordinate_value
            and user_maximum_coordinate_value
            > dataset_maximum_coordinate_value
        )
        or user_minimum_coordinate_value < dataset_minimum_coordinate_value
        or user_maximum_coordinate_value > dataset_maximum_coordinate_value
    ):
        if is_strict:
            raise CoordinatesOutOfDatasetBounds(message)
        else:
            logger.warning(message)
