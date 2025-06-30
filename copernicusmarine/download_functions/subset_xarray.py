import logging
import typing
from datetime import datetime
from decimal import Decimal
from typing import Any, List, Literal, Optional, Union

import numpy
import xarray
from dateutil.tz import UTC

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineCoordinate,
    CopernicusMarinePart,
    CopernicusMarineService,
    CopernicusMarineServiceNames,
)
from copernicusmarine.core_functions.exceptions import (
    CoordinatesOutOfDatasetBounds,
    MinimumLongitudeGreaterThanMaximumLongitude,
    ServiceNotSupported,
    VariableDoesNotExistInTheDataset,
)
from copernicusmarine.core_functions.models import CoordinatesSelectionMethod
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.utils import (
    timestamp_or_datestring_to_datetime,
)
from copernicusmarine.download_functions.subset_parameters import (
    DepthParameters,
    GeographicalParameters,
    TemporalParameters,
    XParameters,
    YParameters,
)
from copernicusmarine.download_functions.utils import (
    get_coordinate_ids_from_parameters,
)

logger = logging.getLogger("copernicusmarine")


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
        and method == "nearest"
    ):
        external_point = dataset.sel(
            {coord_label: actual_extreme}, method=method
        )[coord_label].values
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
    coordinate_label: str,
    coord_selection: Union[float, slice, datetime, None],
    coordinates_selection_method: CoordinatesSelectionMethod,
) -> xarray.Dataset:
    if coordinate_label in dataset.sizes:
        if isinstance(coord_selection, slice):
            if (
                len(dataset[coordinate_label].values) > 1
                and (
                    dataset[coordinate_label].values[0]
                    > dataset[coordinate_label].values[1]
                )
                and coord_selection.start < coord_selection.stop
            ):
                coord_selection = slice(
                    coord_selection.stop, coord_selection.start
                )
        if coordinates_selection_method == "outside":
            if (
                isinstance(coord_selection, slice)
                and coord_selection.stop is not None
            ):
                coord_selection = _enlarge_selection(
                    dataset, coordinate_label, coord_selection
                )
        if coordinates_selection_method == "nearest":
            if (
                isinstance(coord_selection, slice)
                and coord_selection.stop is not None
            ):
                coord_selection = _nearest_selection(
                    dataset, coordinate_label, coord_selection
                )
        if isinstance(coord_selection, slice):
            tmp_dataset = dataset.sel(
                {coordinate_label: coord_selection}, method=None
            )
        else:
            tmp_dataset = dataset.sel(
                {coordinate_label: coord_selection}, method="nearest"
            )
        if tmp_dataset.coords[coordinate_label].size == 0 or (
            coordinate_label not in tmp_dataset.sizes
        ):
            target = (
                coord_selection.start
                if isinstance(coord_selection, slice)
                else coord_selection
            )
            nearest_neighbour_value = dataset.sel(
                {coordinate_label: target}, method="nearest"
            )[coordinate_label].values
            dataset = dataset.sel(
                {
                    coordinate_label: slice(
                        nearest_neighbour_value, nearest_neighbour_value
                    )
                }
            )
        else:
            dataset = tmp_dataset
    return dataset


def get_size_of_coordinate_subset(
    dataset: xarray.Dataset,
    coordinate_label: str,
    minimum: Optional[Union[float, datetime]],
    maximum: Optional[Union[float, datetime]],
) -> int:
    if coordinate_label in dataset.sizes:
        return (
            dataset.coords[coordinate_label]
            .sel({coordinate_label: slice(minimum, maximum)}, method=None)
            .coords[coordinate_label]
            .size
        )
    else:
        raise KeyError(
            f"Could not subset on {coordinate_label}. "
            "Didn't find an equivalent in the dataset."
        )


def _shift_longitude_dimension(
    dataset: xarray.Dataset,
    minimum_longitude_modulus: float,
    coordinate_id: str,
    coordinates_selection_method: CoordinatesSelectionMethod,
):
    if coordinates_selection_method == "outside":
        minimum_longitude_modulus = _choose_extreme_point(
            dataset,
            coordinate_id,
            minimum_longitude_modulus,
            "pad",
        )  # type: ignore
    if coordinates_selection_method == "nearest":
        minimum_longitude_modulus = _choose_extreme_point(
            dataset,
            coordinate_id,
            minimum_longitude_modulus,
            "nearest",
        )  # type: ignore
    window = (
        minimum_longitude_modulus + 180
    )  # compute the degrees needed to move the dataset
    if coordinate_id in dataset.sizes:
        attrs = dataset[coordinate_id].attrs
        if "valid_min" in attrs:
            attrs["valid_min"] += window
        if "valid_max" in attrs:
            attrs["valid_max"] += window
        dataset = dataset.assign_coords(
            {
                coordinate_id: (
                    (dataset[coordinate_id] + (180 - window)) % 360
                )
                - (180 - window)
            }
        ).sortby(coordinate_id)
        dataset[coordinate_id].attrs = attrs
    return dataset


def _y_axis_subset(
    dataset: xarray.Dataset,
    y_parameters: YParameters,
    coordinates_selection_method: CoordinatesSelectionMethod,
) -> xarray.Dataset:
    minimum_y = y_parameters.minimum_y
    maximum_y = y_parameters.maximum_y
    if minimum_y is not None or maximum_y is not None:
        y_selection = (
            minimum_y
            if minimum_y == maximum_y
            else slice(minimum_y, maximum_y)
        )
        return _dataset_custom_sel(
            dataset,
            y_parameters.coordinate_id,
            y_selection,
            coordinates_selection_method,
        )

    return dataset


def x_axis_selection(
    longitude_parameters: XParameters,
) -> tuple[Union[float, slice, None], bool]:
    shift_window = False
    minimum_x = longitude_parameters.minimum_x
    maximum_x = longitude_parameters.maximum_x
    if minimum_x is not None and maximum_x is not None:
        if longitude_parameters.coordinate_id == "longitude":
            if minimum_x > maximum_x:
                raise MinimumLongitudeGreaterThanMaximumLongitude(
                    "--minimum-longitude option must be smaller "
                    "or equal to --maximum-longitude"
                )
            if maximum_x - minimum_x >= 360:
                if maximum_x != 180:
                    shift_window = True
                return slice(-180, 180), shift_window
            else:
                minimum_x = longitude_modulus(minimum_x)
                maximum_x = longitude_modulus(maximum_x)

            if maximum_x and minimum_x is not None and maximum_x < minimum_x:
                maximum_x += 360
                shift_window = True

        return (
            minimum_x
            if minimum_x == maximum_x
            else slice(minimum_x, maximum_x)
        ), shift_window
    if minimum_x is not None:
        return slice(minimum_x, None), shift_window
    elif maximum_x is not None:
        return slice(None, maximum_x), shift_window

    return None, shift_window


def _x_axis_subset(
    dataset: xarray.Dataset,
    longitude_parameters: XParameters,
    coordinates_selection_method: CoordinatesSelectionMethod,
) -> xarray.Dataset:
    (x_selection, shift_window) = x_axis_selection(longitude_parameters)
    if shift_window and isinstance(x_selection, slice):
        dataset = _shift_longitude_dimension(
            dataset,
            x_selection.start,
            longitude_parameters.coordinate_id,
            coordinates_selection_method,
        )

    if x_selection is not None:

        return _dataset_custom_sel(
            dataset,
            longitude_parameters.coordinate_id,
            x_selection,
            coordinates_selection_method,
        )
    return dataset


def t_axis_selection(
    temporal_parameters: TemporalParameters,
) -> Union[slice, datetime, None]:

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
        return (
            start_datetime
            if start_datetime == end_datetime
            else slice(start_datetime, end_datetime)
        )
    return None


def _temporal_subset(
    dataset: xarray.Dataset,
    temporal_parameters: TemporalParameters,
    coordinates_selection_method: CoordinatesSelectionMethod,
) -> xarray.Dataset:
    temporal_selection = t_axis_selection(temporal_parameters)
    if temporal_selection is not None:
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
            depth_parameters.vertical_axis,
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
    coordinate_ids: list[str],
) -> xarray.Dataset:
    for coordinate_id in coordinate_ids:
        if coordinate_id in dataset.sizes:
            coord = dataset[coordinate_id]
            attrs = coord.attrs
            coordinate_attributes = (
                NETCDF_CONVENTION_COORDINATE_ATTRIBUTES.copy()
            )
            if "time" in coordinate_id:
                attrs["standard_name"] = "time"
                attrs["long_name"] = "Time"
                attrs["axis"] = "T"
                attrs["unit_long"] = (
                    coord.encoding["units"].replace("_", " ").title()
                )
                coordinate_attributes.remove("units")
            elif coordinate_id in ["depth", "elevation"]:
                coordinate_attributes.append("positive")
            # TODO: delete this when fixed on ARCO processor side
            # example dataset: esa_obs-si_arc_phy-sit_nrt_l4-multi_P1D-m
            if attrs == {}:
                if coordinate_id == "longitude":
                    attrs = {
                        "standard_name": "longitude",
                        "long_name": "Longitude",
                        "units": "degrees_east",
                        "units_long": "Degrees East",
                        "axis": "X",
                    }
                elif coordinate_id == "latitude":
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
    dataset = _y_axis_subset(
        dataset,
        geographical_parameters.y_axis_parameters,
        coordinates_selection_method,
    )
    dataset = _x_axis_subset(
        dataset,
        geographical_parameters.x_axis_parameters,
        coordinates_selection_method,
    )

    dataset = _temporal_subset(
        dataset, temporal_parameters, coordinates_selection_method
    )

    dataset = _depth_subset(
        dataset,
        depth_parameters,
        coordinates_selection_method,
    )

    dataset = _update_dataset_coordinate_attributes(
        dataset,
        get_coordinate_ids_from_parameters(
            geographical_parameters, temporal_parameters, depth_parameters
        ),
    )

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
    service: CopernicusMarineService,
    part: CopernicusMarinePart,
    dataset_subset: SubsetRequest,
    coordinates_selection_method: CoordinatesSelectionMethod,
    axis_coordinate_id_mapping: dict[str, str],
) -> None:
    # TODO: check lon/lat for original grid
    if service.service_name not in [
        CopernicusMarineServiceNames.GEOSERIES,
        CopernicusMarineServiceNames.TIMESERIES,
        CopernicusMarineServiceNames.OMI_ARCO,
        CopernicusMarineServiceNames.STATIC_ARCO,
        CopernicusMarineServiceNames.PLATFORMSERIES,
    ]:
        raise ServiceNotSupported(service.service_name)
    all_coordinates = part.get_coordinates()
    if "y" in axis_coordinate_id_mapping:
        coordinate_id = axis_coordinate_id_mapping["y"]
        coordinate, _, _ = all_coordinates[coordinate_id]
        minimum_value, maximum_value = _get_minimun_maximum_dataset(coordinate)
        user_minimum_coordinate_value = (
            dataset_subset.minimum_y
            if dataset_subset.minimum_y is not None
            else minimum_value
        )
        user_maximum_coordinate_value = (
            dataset_subset.maximum_y
            if dataset_subset.maximum_y is not None
            else maximum_value
        )
        _check_coordinate_overlap(
            dimension=coordinate_id,
            user_minimum_coordinate_value=user_minimum_coordinate_value,
            user_maximum_coordinate_value=user_maximum_coordinate_value,
            dataset_minimum_coordinate_value=minimum_value,
            dataset_maximum_coordinate_value=maximum_value,
            is_strict=coordinates_selection_method == "strict-inside",
        )
    if "x" in axis_coordinate_id_mapping:
        coordinate_id = axis_coordinate_id_mapping["x"]
        coordinate, _, _ = all_coordinates[coordinate_id]
        minimum_value, maximum_value = _get_minimun_maximum_dataset(coordinate)
        if coordinate_id == "longitude":
            if (
                dataset_subset.minimum_x is not None
                and dataset_subset.maximum_x is not None
                and dataset_subset.minimum_x > dataset_subset.maximum_x
            ):
                raise MinimumLongitudeGreaterThanMaximumLongitude(
                    "--minimum-longitude option must be smaller "
                    "or equal to --maximum-longitude"
                )
            user_minimum_coordinate_value = (
                longitude_modulus(dataset_subset.minimum_x)
                if dataset_subset.minimum_x is not None
                else minimum_value
            )
            user_maximum_coordinate_value = (
                longitude_modulus_upper_bound(dataset_subset.maximum_x)
                if dataset_subset.maximum_x is not None
                else maximum_value
            )
        else:
            user_minimum_coordinate_value = (
                dataset_subset.minimum_x
                if dataset_subset.minimum_x is not None
                else minimum_value
            )
            user_maximum_coordinate_value = (
                dataset_subset.maximum_x
                if dataset_subset.maximum_x is not None
                else maximum_value
            )
        _check_coordinate_overlap(
            dimension=coordinate_id,
            user_minimum_coordinate_value=user_minimum_coordinate_value,
            user_maximum_coordinate_value=user_maximum_coordinate_value,
            dataset_minimum_coordinate_value=minimum_value,
            dataset_maximum_coordinate_value=maximum_value,
            is_strict=coordinates_selection_method == "strict-inside",
        )
    if "t" in axis_coordinate_id_mapping:
        coordinate_id = axis_coordinate_id_mapping["t"]
        coordinate, _, _ = all_coordinates[coordinate_id]
        minimum_value, maximum_value = _get_minimun_maximum_dataset(coordinate)
        dataset_minimum_coordinate_value = timestamp_or_datestring_to_datetime(
            minimum_value
        )
        dataset_maximum_coordinate_value = timestamp_or_datestring_to_datetime(
            maximum_value
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
    if "z" in axis_coordinate_id_mapping:
        coordinate_id = axis_coordinate_id_mapping["z"]
        coordinate, _, _ = all_coordinates[coordinate_id]
        minimum_value, maximum_value = _get_minimun_maximum_dataset(coordinate)
        _check_coordinate_overlap(
            dimension="depth",
            user_minimum_coordinate_value=(
                dataset_subset.minimum_depth
                if dataset_subset.minimum_depth is not None
                else minimum_value
            ),
            user_maximum_coordinate_value=(
                dataset_subset.maximum_depth
                if dataset_subset.maximum_depth is not None
                else maximum_value
            ),
            dataset_minimum_coordinate_value=minimum_value,
            dataset_maximum_coordinate_value=maximum_value,
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
    if dataset_maximum_coordinate_value < dataset_minimum_coordinate_value:
        dataset_maximum_coordinate_value, dataset_minimum_coordinate_value = (
            dataset_minimum_coordinate_value,
            dataset_maximum_coordinate_value,
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


def _get_minimun_maximum_dataset(
    coordinate: CopernicusMarineCoordinate,
) -> tuple[Any, Any]:
    maximum_value = coordinate.maximum_value
    minimum_value = coordinate.minimum_value
    values = coordinate.values

    if maximum_value is None and values:
        maximum_value = max(values)
    if minimum_value is None and values:
        minimum_value = min(values)
    return minimum_value, maximum_value
