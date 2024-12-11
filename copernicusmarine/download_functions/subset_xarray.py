import logging
import typing
from datetime import datetime
from decimal import Decimal
from typing import List, Literal, Optional, Union

import numpy
import xarray
from dateutil.tz import UTC

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineServiceNames,
)
from copernicusmarine.catalogue_parser.request_structure import (
    DatasetTimeAndSpaceSubset,
)
from copernicusmarine.core_functions import custom_open_zarr
from copernicusmarine.core_functions.exceptions import (
    CoordinatesOutOfDatasetBounds,
    GeospatialSubsetNotAvailableForNonLatLon,
    MinimumLongitudeGreaterThanMaximumLongitude,
    ServiceNotSupported,
    VariableDoesNotExistInTheDataset,
)
from copernicusmarine.core_functions.models import CoordinatesSelectionMethod
from copernicusmarine.core_functions.utils import (
    timestamp_or_datestring_to_datetime,
)
from copernicusmarine.download_functions.subset_parameters import (
    DepthParameters,
    GeographicalParameters,
    LatitudeParameters,
    LongitudeParameters,
    TemporalParameters,
)

logger = logging.getLogger("copernicusmarine")

COORDINATES_LABEL = {
    "latitude": ["latitude", "nav_lat", "x", "lat"],
    "longitude": ["longitude", "nav_lon", "y", "lon"],
    "time": ["time_counter", "time"],
    "depth": ["depth", "deptht", "elevation"],
}

NETCDF_CONVENTION_VARIABLE_ATTRIBUTES = [
    "standard_name",
    "long_name",
    "units",
    "unit_long",
    "valid_min",
    "valid_max",
]
NETCDF_CONVENTION_COORDINATE_ATTRIBUTES = [
    "standard_name",
    "long_name",
    "units",
    "unit_long",
    "axis",
]
NETCDF_CONVENTION_DATASET_ATTRIBUTES = [
    "title",
    "institution",
    "source",
    "history",
    "references",
    "comment",
    "Conventions",
    "producer",
    "credit",
    "contact",
]


@typing.no_type_check
def _choose_extreme_point(
    dataset: xarray.Dataset,
    coord_label: str,
    actual_extreme: Union[float, datetime],
    method: Literal["pad", "backfill", "nearest"],
) -> Union[float, datetime]:
    if (
        coord_label == "time"
        and actual_extreme
        >= timestamp_or_datestring_to_datetime(
            dataset[coord_label].values.min()
        ).replace(tzinfo=None)
        and actual_extreme
        <= timestamp_or_datestring_to_datetime(
            dataset[coord_label].values.max()
        ).replace(tzinfo=None)
    ):
        external_point = dataset.sel(
            {coord_label: actual_extreme}, method=method
        )[coord_label].values
        external_point = timestamp_or_datestring_to_datetime(
            external_point
        ).replace(tzinfo=None)
    elif (
        coord_label != "time"
        and actual_extreme > dataset[coord_label].min()
        and actual_extreme < dataset[coord_label].max()
    ):
        external_point = dataset.sel(
            {coord_label: actual_extreme}, method=method
        )[coord_label].values
    else:
        external_point = actual_extreme
    return external_point


def _enlarge_selection(
    dataset: xarray.Dataset,
    coord_label: str,
    coord_selection: slice,
) -> slice:
    external_minimum = _choose_extreme_point(
        dataset, coord_label, coord_selection.start, "pad"
    )

    external_maximum = _choose_extreme_point(
        dataset, coord_label, coord_selection.stop, "backfill"
    )

    return slice(external_minimum, external_maximum)


def _nearest_selection(
    dataset: xarray.Dataset,
    coord_label: str,
    coord_selection: slice,
) -> slice:
    external_minimum = _choose_extreme_point(
        dataset, coord_label, coord_selection.start, "nearest"
    )

    external_maximum = _choose_extreme_point(
        dataset, coord_label, coord_selection.stop, "nearest"
    )

    return slice(external_minimum, external_maximum)


def _dataset_custom_sel(
    dataset: xarray.Dataset,
    coord_type: Literal["latitude", "longitude", "depth", "time"],
    coord_selection: Union[float, slice, datetime, None],
    coordinates_selection_method: CoordinatesSelectionMethod,
) -> xarray.Dataset:
    for coord_label in COORDINATES_LABEL[coord_type]:
        if coord_label in dataset.sizes:
            if coordinates_selection_method == "outside":
                if (
                    isinstance(coord_selection, slice)
                    and coord_selection.stop is not None
                ):
                    coord_selection = _enlarge_selection(
                        dataset, coord_label, coord_selection
                    )
            if coordinates_selection_method == "nearest":
                if (
                    isinstance(coord_selection, slice)
                    and coord_selection.stop is not None
                ):
                    coord_selection = _nearest_selection(
                        dataset, coord_label, coord_selection
                    )
            if isinstance(coord_selection, slice):
                tmp_dataset = dataset.sel(
                    {coord_label: coord_selection}, method=None
                )
            else:
                tmp_dataset = dataset.sel(
                    {coord_label: coord_selection}, method="nearest"
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
    for coordinate_label in COORDINATES_LABEL[coordinate]:
        if coordinate_label in dataset.sizes:
            return (
                dataset.coords[coordinate_label]
                .sel({coordinate_label: slice(minimum, maximum)}, method=None)
                .coords[coordinate_label]
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
    coordinates_selection_method: CoordinatesSelectionMethod,
):
    if coordinates_selection_method == "outside":
        minimum_longitude_modulus = _choose_extreme_point(
            dataset,
            "longitude",
            minimum_longitude_modulus,
            "pad",
        )  # type: ignore
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
    coordinates_selection_method: CoordinatesSelectionMethod,
) -> xarray.Dataset:
    minimum_latitude = latitude_parameters.minimum_latitude
    maximum_latitude = latitude_parameters.maximum_latitude
    if minimum_latitude is not None or maximum_latitude is not None:
        latitude_selection = (
            minimum_latitude
            if minimum_latitude == maximum_latitude
            else slice(minimum_latitude, maximum_latitude)
        )
        dataset = _dataset_custom_sel(
            dataset,
            "latitude",
            latitude_selection,
            coordinates_selection_method,
        )

    return dataset


def _longitude_subset(
    dataset: xarray.Dataset,
    longitude_parameters: LongitudeParameters,
    coordinates_selection_method: CoordinatesSelectionMethod,
) -> xarray.Dataset:
    longitude_moduli = apply_longitude_modulus(longitude_parameters)
    if longitude_moduli is None:
        return dataset
    minimum_longitude_modulus, maximum_longitude_modulus = longitude_moduli
    if (
        maximum_longitude_modulus
        and minimum_longitude_modulus
        and maximum_longitude_modulus < minimum_longitude_modulus
    ):
        maximum_longitude_modulus += 360
        dataset = _shift_longitude_dimension(
            dataset,
            minimum_longitude_modulus,
            coordinates_selection_method,
        )

    longitude_selection = slice(
        minimum_longitude_modulus,
        maximum_longitude_modulus,
    )

    return _dataset_custom_sel(
        dataset,
        "longitude",
        longitude_selection,
        coordinates_selection_method,
    )


def apply_longitude_modulus(
    longitude_parameters: LongitudeParameters,
) -> Optional[tuple[Optional[float], Optional[float]]]:
    minimum_longitude = longitude_parameters.minimum_longitude
    maximum_longitude = longitude_parameters.maximum_longitude
    if minimum_longitude is None and maximum_longitude is None:
        return None
    if minimum_longitude is not None and maximum_longitude is not None:
        if minimum_longitude > maximum_longitude:
            raise MinimumLongitudeGreaterThanMaximumLongitude(
                "--minimum-longitude option must be smaller "
                "or equal to --maximum-longitude"
            )
        if maximum_longitude - minimum_longitude >= 360:
            return None
        else:
            minimum_longitude_modulus = longitude_modulus(minimum_longitude)
            maximum_longitude_modulus = longitude_modulus(maximum_longitude)
            return minimum_longitude_modulus, maximum_longitude_modulus

    else:
        return minimum_longitude, maximum_longitude


def _temporal_subset(
    dataset: xarray.Dataset,
    temporal_parameters: TemporalParameters,
    coordinates_selection_method: CoordinatesSelectionMethod,
) -> xarray.Dataset:
    start_datetime = (
        temporal_parameters.start_datetime.astimezone(UTC).replace(tzinfo=None)
        if temporal_parameters.start_datetime
        else temporal_parameters.start_datetime
    )
    end_datetime = (
        temporal_parameters.end_datetime.astimezone(UTC).replace(tzinfo=None)
        if temporal_parameters.end_datetime
        else temporal_parameters.end_datetime
    )
    if start_datetime is not None or end_datetime is not None:
        temporal_selection = (
            start_datetime
            if start_datetime == end_datetime
            else slice(start_datetime, end_datetime)
        )
        dataset = _dataset_custom_sel(
            dataset,
            "time",
            temporal_selection,
            coordinates_selection_method,
        )
    return dataset


def _depth_subset(
    dataset: xarray.Dataset,
    depth_parameters: DepthParameters,
    coordinates_selection_method: CoordinatesSelectionMethod,
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

    if depth_parameters.vertical_axis == "depth":
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
        dataset = _dataset_custom_sel(
            dataset,
            "depth",
            depth_selection,
            coordinates_selection_method,
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


def _adequate_dtypes_of_valid_minmax(
    dataset: xarray.Dataset, variable: str
) -> xarray.Dataset:
    dataset[variable].attrs["valid_min"] = numpy.array(
        [dataset[variable].attrs["valid_min"]],
        dtype=dataset[variable].encoding["dtype"],
    )[0]
    dataset[variable].attrs["valid_max"] = numpy.array(
        [dataset[variable].attrs["valid_max"]],
        dtype=dataset[variable].encoding["dtype"],
    )[0]
    return dataset


def _update_variables_attributes(
    dataset: xarray.Dataset, variables: List[str]
) -> xarray.Dataset:
    for variable in variables:
        dataset[variable].attrs = _filter_attributes(
            dataset[variable].attrs, NETCDF_CONVENTION_VARIABLE_ATTRIBUTES
        )
        if (
            "valid_min" in dataset[variable].attrs
            and "valid_max" in dataset[variable].attrs
        ):
            _adequate_dtypes_of_valid_minmax(dataset, variable)
    return dataset


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
    dataset = dataset[numpy.array(dataset_variables_filter)]
    return _update_variables_attributes(dataset, dataset_variables_filter)


def _filter_attributes(attributes: dict, attributes_to_keep: List[str]):
    attributes_that_exist = set(attributes).intersection(attributes_to_keep)
    return {key: attributes[key] for key in attributes_that_exist}


def _update_dataset_coordinate_attributes(
    dataset: xarray.Dataset,
) -> xarray.Dataset:
    for coordinate_label in COORDINATES_LABEL:
        for coordinate_alias in COORDINATES_LABEL[coordinate_label]:
            if coordinate_alias in dataset.sizes:
                coord = dataset[coordinate_alias]
                attrs = coord.attrs
                coordinate_attributes = (
                    NETCDF_CONVENTION_COORDINATE_ATTRIBUTES.copy()
                )
                if "time" in coordinate_label:
                    attrs["standard_name"] = "time"
                    attrs["long_name"] = "Time"
                    attrs["axis"] = "T"
                    attrs["unit_long"] = (
                        coord.encoding["units"].replace("_", " ").title()
                    )
                    coordinate_attributes.remove("units")
                elif coordinate_label in ["depth", "elevation"]:
                    coordinate_attributes.append("positive")
                # TODO: delete this when fixed on ARCO processor side
                # example dataset: esa_obs-si_arc_phy-sit_nrt_l4-multi_P1D-m
                if attrs == {}:
                    if coordinate_alias == "longitude":
                        attrs = {
                            "standard_name": "longitude",
                            "long_name": "Longitude",
                            "units": "degrees_east",
                            "units_long": "Degrees East",
                            "axis": "X",
                        }
                    elif coordinate_alias == "latitude":
                        attrs = {
                            "standard_name": "latitude",
                            "long_name": "Latitude",
                            "units": "degrees_north",
                            "units_long": "Degrees North",
                            "axis": "Y",
                        }
                coord.attrs = _filter_attributes(attrs, coordinate_attributes)

    dataset.attrs = _filter_attributes(
        dataset.attrs, NETCDF_CONVENTION_DATASET_ATTRIBUTES
    )

    return dataset


def subset(
    dataset: xarray.Dataset,
    variables: Optional[List[str]],
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
    coordinates_selection_method: CoordinatesSelectionMethod,
) -> xarray.Dataset:
    if variables:
        dataset = _variables_subset(dataset, variables)

    dataset = _latitude_subset(
        dataset,
        geographical_parameters.latitude_parameters,
        coordinates_selection_method,
    )
    dataset = _longitude_subset(
        dataset,
        geographical_parameters.longitude_parameters,
        coordinates_selection_method,
    )

    dataset = _temporal_subset(
        dataset, temporal_parameters, coordinates_selection_method
    )

    dataset = _depth_subset(
        dataset, depth_parameters, coordinates_selection_method
    )

    dataset = _update_dataset_coordinate_attributes(dataset)

    return dataset


def longitude_modulus(longitude: float) -> float:
    """
    Returns the equivalent longitude in [-180, 180[
    """
    # We are using Decimal to avoid issue with rounding
    modulus = (Decimal(str(longitude)) + 180) % 360
    # Modulus with python return a negative value if the denominator is negative
    # To counteract that, we add 360 if the result is < 0
    modulus = modulus if modulus >= 0 else modulus + 360
    return float(modulus - 180)


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
    service_name: CopernicusMarineServiceNames,
    dataset_subset: DatasetTimeAndSpaceSubset,
    coordinates_selection_method: CoordinatesSelectionMethod,
    dataset_valid_date: Optional[Union[str, int, float]],
    is_original_grid: bool,
) -> None:
    if service_name in [
        CopernicusMarineServiceNames.GEOSERIES,
        CopernicusMarineServiceNames.TIMESERIES,
        CopernicusMarineServiceNames.OMI_ARCO,
        CopernicusMarineServiceNames.STATIC_ARCO,
    ]:
        dataset = custom_open_zarr.open_zarr(
            dataset_url, copernicus_marine_username=username
        )
        dataset_coordinates = dataset.coords
    else:
        raise ServiceNotSupported(service_name)
    if is_original_grid:
        logger.debug("Dataset part has the non lat lon projection.")
        if (
            dataset_subset.minimum_latitude is not None
            or dataset_subset.maximum_latitude is not None
            or dataset_subset.minimum_longitude is not None
            or dataset_subset.maximum_longitude is not None
        ):
            raise GeospatialSubsetNotAvailableForNonLatLon()

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
                is_strict=coordinates_selection_method == "strict-inside",
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
                is_strict=coordinates_selection_method == "strict-inside",
            )
    for coordinate_label in COORDINATES_LABEL["time"]:
        if coordinate_label in dataset.sizes:
            times = dataset_coordinates[coordinate_label].values
            if dataset_valid_date:
                times_min = dataset_valid_date
            else:
                times_min = times.min()
            dataset_minimum_coordinate_value = (
                timestamp_or_datestring_to_datetime(times_min)
            )
            dataset_maximum_coordinate_value = (
                timestamp_or_datestring_to_datetime(times.max())
            )
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
                is_strict=coordinates_selection_method == "strict-inside",
            )
    for coordinate_label in COORDINATES_LABEL["depth"]:
        if coordinate_label in dataset.sizes:
            depths = -1 * dataset_coordinates[coordinate_label].values
            _check_coordinate_overlap(
                dimension="depth",
                user_minimum_coordinate_value=(
                    dataset_subset.minimum_depth
                    if dataset_subset.minimum_depth is not None
                    else depths.min()
                ),
                user_maximum_coordinate_value=(
                    dataset_subset.maximum_depth
                    if dataset_subset.maximum_depth is not None
                    else depths.max()
                ),
                dataset_minimum_coordinate_value=depths.min(),
                dataset_maximum_coordinate_value=depths.max(),
                is_strict=coordinates_selection_method == "strict-inside",
            )


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
        f"Some of your subset selection "
        f"[{user_minimum_coordinate_value}, {user_maximum_coordinate_value}] "
        f"for the {dimension} dimension exceed the dataset coordinates "
        f"[{dataset_minimum_coordinate_value}, "
        f"{dataset_maximum_coordinate_value}]"
    )
    if dimension == "longitude":
        if dataset_minimum_coordinate_value == -180:
            dataset_maximum_coordinate_value = 180
        if dataset_maximum_coordinate_value == 180:
            dataset_minimum_coordinate_value = -180
    if user_maximum_coordinate_value < dataset_minimum_coordinate_value:
        if user_minimum_coordinate_value == dataset_minimum_coordinate_value:
            message = (
                f"Some of your subset selection "
                f"({dimension} < {user_maximum_coordinate_value}) "
                f"for the {dimension} dimension exceed the dataset coordinates "
                f"[{dataset_minimum_coordinate_value}, "
                f"{dataset_maximum_coordinate_value}]"
            )
        raise CoordinatesOutOfDatasetBounds(message)
    elif user_minimum_coordinate_value > dataset_maximum_coordinate_value:
        if user_maximum_coordinate_value == dataset_maximum_coordinate_value:
            message = (
                f"Some of your subset selection "
                f"({dimension} > {user_minimum_coordinate_value}) "
                f"for the {dimension} dimension exceed the dataset coordinates "
                f"[{dataset_minimum_coordinate_value}, "
                f"{dataset_maximum_coordinate_value}]"
            )
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
