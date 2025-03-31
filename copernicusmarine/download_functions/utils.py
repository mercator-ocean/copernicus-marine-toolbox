import bisect
import logging
import math
import pathlib
from datetime import datetime
from typing import Any, Optional, Union

import xarray

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineCoordinate,
    CopernicusMarineService,
)
from copernicusmarine.core_functions.models import (
    DEFAULT_FILE_EXTENSIONS,
    FileFormat,
    GeographicalExtent,
    TimeExtent,
)
from copernicusmarine.core_functions.request_structure import SubsetRequest
from copernicusmarine.core_functions.utils import (
    timestamp_or_datestring_to_datetime,
)
from copernicusmarine.download_functions.subset_parameters import (
    DepthParameters,
    GeographicalParameters,
    TemporalParameters,
)

logger = logging.getLogger("copernicusmarine")


def get_file_extension(file_format: FileFormat) -> str:
    if file_format == "zarr":
        return ".zarr"
    if file_format == "csv":
        return ".csv"
    if file_format == "parquet":
        return ".parquet"
    else:
        return ".nc"


def get_filename(
    filename: Optional[str],
    dataset: xarray.Dataset,
    dataset_id: str,
    file_format: FileFormat,
    axis_coordinate_id_mapping: dict[str, str],
    geographical_parameters: GeographicalParameters,
) -> str:
    if filename:
        if pathlib.Path(filename).suffix in DEFAULT_FILE_EXTENSIONS:
            return filename
        else:
            return filename + get_file_extension(file_format)
    else:
        return _build_filename_from_dataset(
            dataset,
            dataset_id,
            file_format,
            axis_coordinate_id_mapping,
            geographical_parameters,
        )


def _build_filename_from_dataset(
    dataset: xarray.Dataset,
    dataset_id: str,
    file_format: FileFormat,
    axis_coordinate_id_mapping: dict[str, str],
    geographical_parameters: GeographicalParameters,
) -> str:
    dataset_variables = "-".join(
        [str(variable_name) for variable_name in dataset.data_vars]
    )
    variables = (
        "multi-vars"
        if (len(dataset_variables) > 15 and len(list(dataset.keys())) > 1)
        else dataset_variables
    )
    longitudes = None
    if "x" in axis_coordinate_id_mapping:
        if geographical_parameters.projection == "lonlat":
            longitudes = _format_longitudes(
                _get_min_coordinate(dataset, axis_coordinate_id_mapping["x"]),
                _get_max_coordinate(dataset, axis_coordinate_id_mapping["x"]),
            )
        if geographical_parameters.projection == "originalGrid":
            longitudes = _format_xy_axis(
                _get_min_coordinate(dataset, axis_coordinate_id_mapping["x"]),
                _get_max_coordinate(dataset, axis_coordinate_id_mapping["x"]),
                "x",
            )

    latitudes = None
    if "y" in axis_coordinate_id_mapping:
        if geographical_parameters.projection == "lonlat":
            latitudes = _format_latitudes(
                _get_min_coordinate(dataset, axis_coordinate_id_mapping["y"]),
                _get_max_coordinate(dataset, axis_coordinate_id_mapping["y"]),
            )
        if geographical_parameters.projection == "originalGrid":
            latitudes = _format_xy_axis(
                _get_min_coordinate(dataset, axis_coordinate_id_mapping["y"]),
                _get_max_coordinate(dataset, axis_coordinate_id_mapping["y"]),
                "y",
            )

    depths = None
    if "z" in axis_coordinate_id_mapping:
        depths = _format_depths(
            _get_min_coordinate(dataset, axis_coordinate_id_mapping["z"]),
            _get_max_coordinate(dataset, axis_coordinate_id_mapping["z"]),
        )

    datetimes = None
    if "t" in axis_coordinate_id_mapping:
        min_time_coordinate = _get_min_coordinate(
            dataset, axis_coordinate_id_mapping["t"]
        )
        max_time_coordinate = _get_max_coordinate(
            dataset, axis_coordinate_id_mapping["t"]
        )
        datetimes = _format_datetimes(
            (
                timestamp_or_datestring_to_datetime(min_time_coordinate)
                if min_time_coordinate is not None
                else None
            ),
            (
                timestamp_or_datestring_to_datetime(max_time_coordinate)
                if max_time_coordinate is not None
                else None
            ),
        )
    filename = "_".join(
        filter(
            None,
            [dataset_id, variables, longitudes, latitudes, depths, datetimes],
        )
    )
    filename = filename if len(filename) < 250 else filename[250:]

    return filename + get_file_extension(file_format)


def build_filename_from_request(
    request: SubsetRequest,
    variables: list[str],
    platform_ids: list[str],
    axis_coordinate_id_mapping: dict[str, str],
) -> str:
    """
    In the sparse dataset case we don't have the dataset to build the filename from.
    """  # noqa

    dataset_variables = "-".join(variables)
    dataset_variables = (
        "multi-vars" if len(variables) > 15 else dataset_variables
    )
    platform_ids_text = "-".join(platform_ids)
    platform_ids_text = (
        "multi-platforms" if len(platform_ids) > 15 else platform_ids_text
    )
    longitudes = None
    if "x" in axis_coordinate_id_mapping:
        if axis_coordinate_id_mapping["x"] == "longitude":
            longitudes = _format_longitudes(
                request.minimum_x, request.maximum_x
            )
        if axis_coordinate_id_mapping["x"] == "x":
            longitudes = _format_xy_axis(
                request.minimum_x,
                request.maximum_x,
                axis_coordinate_id_mapping["x"],
            )

    latitudes = None
    if "y" in axis_coordinate_id_mapping:
        if axis_coordinate_id_mapping["y"] == "latitude":
            latitudes = _format_latitudes(request.minimum_y, request.maximum_y)
        if axis_coordinate_id_mapping["y"] == "y":
            latitudes = _format_xy_axis(
                request.minimum_y,
                request.maximum_y,
                axis_coordinate_id_mapping["y"],
            )

    depths = None
    if "z" in axis_coordinate_id_mapping:
        if axis_coordinate_id_mapping["z"] == "depth":
            depths = _format_depths(
                request.minimum_depth, request.maximum_depth
            )

    datetimes = None
    if "t" in axis_coordinate_id_mapping:
        datetimes = _format_datetimes(
            request.start_datetime, request.end_datetime
        )
    filename = "_".join(
        filter(
            None,
            [
                request.dataset_id,
                dataset_variables,
                platform_ids_text,
                longitudes,
                latitudes,
                depths,
                datetimes,
            ],
        )
    )
    filename = filename if len(filename) < 250 else filename[250:]

    return filename + get_file_extension(request.file_format)


def get_coordinate_ids_from_parameters(
    geographical_parameters: GeographicalParameters,
    temporal_parameters: TemporalParameters,
    depth_parameters: DepthParameters,
) -> list[str]:
    return [
        coordinate_id
        for coordinate_id in [
            geographical_parameters.x_axis_parameters.coordinate_id,
            geographical_parameters.y_axis_parameters.coordinate_id,
            temporal_parameters.coordinate_id,
            depth_parameters.coordinate_id,
        ]
        if coordinate_id
    ]


def _get_min_coordinate(dataset: xarray.Dataset, coordinate_id: str) -> Any:
    if coordinate_id in dataset.sizes:
        return min(dataset[coordinate_id].values)
    return None


def _get_max_coordinate(dataset: xarray.Dataset, coordinate_id: str) -> Any:
    if coordinate_id in dataset.sizes:
        return max(dataset[coordinate_id].values)
    return None


def _get_unit_coordinate(dataset: xarray.Dataset, coordinate_id: str) -> Any:
    if coordinate_id in dataset.sizes:
        return dataset[coordinate_id].attrs.get("units")
    return None


def _format_longitudes(
    minimum_longitude: Optional[float], maximum_longitude: Optional[float]
) -> str:
    if minimum_longitude is None or maximum_longitude is None:
        return ""
    else:
        if minimum_longitude == maximum_longitude:
            suffix = "W" if minimum_longitude < 0 else "E"
            longitude = f"{abs(minimum_longitude):.2f}{suffix}"
        else:
            minimum_suffix = "W" if minimum_longitude < 0 else "E"
            maximum_suffix = "W" if maximum_longitude < 0 else "E"
            longitude = (
                f"{abs(minimum_longitude):.2f}{minimum_suffix}-"
                f"{abs(maximum_longitude):.2f}{maximum_suffix}"
            )
        return longitude


def _format_latitudes(
    minimum_latitude: Optional[float], maximum_latitude: Optional[float]
) -> str:
    if minimum_latitude is None or maximum_latitude is None:
        return ""
    else:
        if minimum_latitude == maximum_latitude:
            suffix = "S" if minimum_latitude < 0 else "N"
            latitude = f"{abs(minimum_latitude):.2f}{suffix}"
        else:
            minimum_suffix = "S" if minimum_latitude < 0 else "N"
            maximum_suffix = "S" if maximum_latitude < 0 else "N"
            latitude = (
                f"{abs(minimum_latitude):.2f}{minimum_suffix}-"
                f"{abs(maximum_latitude):.2f}{maximum_suffix}"
            )
        return latitude


def _format_xy_axis(
    minimum_value: Optional[float],
    maximum_value: Optional[float],
    coordinate_id: str,
) -> str:
    if minimum_value is None or maximum_value is None:
        return ""
    else:
        if minimum_value == maximum_value:
            suffix = coordinate_id
            value = f"{(minimum_value):.2f}{suffix}"
        else:
            minimum_suffix = coordinate_id
            maximum_suffix = coordinate_id.upper()
            value = (
                f"{(minimum_value):.2f}{minimum_suffix}_"
                f"{(maximum_value):.2f}{maximum_suffix}"
            )
        return value


def _format_depths(
    minimum_depth: Optional[float], maximum_depth: Optional[float]
) -> str:
    if minimum_depth is None or maximum_depth is None:
        return ""
    else:
        if minimum_depth == maximum_depth:
            depth = f"{abs(minimum_depth):.2f}m"
        else:
            depth = f"{abs(minimum_depth):.2f}-{abs(maximum_depth):.2f}m"
        return depth


def _format_datetimes(
    minimum_datetime: Optional[datetime], maximum_datetime: Optional[datetime]
) -> str:
    if minimum_datetime is None or maximum_datetime is None:
        return ""
    else:
        if minimum_datetime == maximum_datetime:
            formatted_datetime = f"{minimum_datetime.strftime('%Y-%m-%d')}"
        else:
            formatted_datetime = (
                f"{minimum_datetime.strftime('%Y-%m-%d')}-"
                f"{maximum_datetime.strftime('%Y-%m-%d')}"
            )
        return formatted_datetime


def get_dataset_coordinates_extent(
    dataset: xarray.Dataset, axis_coordinate_id_mapping: dict[str, str]
) -> list[Union[GeographicalExtent, TimeExtent]]:
    coordinates_extent = []
    for coord_axis in ["x", "y", "t", "z"]:
        if coordinate_id := axis_coordinate_id_mapping.get(coord_axis):
            if coordinate_extent := _get_coordinate_extent(
                dataset, coordinate_id
            ):
                coordinates_extent.append(coordinate_extent)

    return coordinates_extent


def _get_coordinate_extent(
    dataset: xarray.Dataset,
    coordinate_id: str,
) -> Optional[Union[GeographicalExtent, TimeExtent]]:
    if coordinate_id in dataset.sizes:
        minimum = _get_min_coordinate(dataset, coordinate_id)
        maximum = _get_max_coordinate(dataset, coordinate_id)
        unit = _get_unit_coordinate(dataset, coordinate_id)
        if coordinate_id == "time":
            minimum = timestamp_or_datestring_to_datetime(minimum).isoformat()
            maximum = timestamp_or_datestring_to_datetime(maximum).isoformat()
            unit = "iso8601"
            return TimeExtent(
                minimum=minimum,
                maximum=maximum,
                unit=unit,
                coordinate_id=coordinate_id,
            )
        return GeographicalExtent(
            minimum=minimum,
            maximum=maximum,
            unit=unit,
            coordinate_id=coordinate_id,
        )
    return None


def get_approximation_size_final_result(
    dataset: xarray.Dataset, axis_coordinate_id_mapping: dict[str, str]
) -> Optional[float]:
    coordinates_size = 1
    variables_size = 0
    baseline_size = 0.013

    for variable in dataset.data_vars:
        variables_size += dataset[variable].encoding["dtype"].itemsize

    for coordinate_name in dataset.sizes:
        for coord_label in axis_coordinate_id_mapping:
            if coordinate_name == axis_coordinate_id_mapping[coord_label]:
                coordinates_size *= dataset[coordinate_name].size
    estimate_size = baseline_size + coordinates_size * variables_size / 1048e3

    return estimate_size


def get_approximation_size_data_downloaded(
    dataset: xarray.Dataset,
    service: CopernicusMarineService,
    axis_coordinate_id_mapping: dict[str, str],
) -> Optional[float]:
    temp_dataset = dataset.copy()
    if "elevation" in dataset.sizes:
        temp_dataset["elevation"] = temp_dataset.elevation * (-1)
        temp_dataset = temp_dataset.rename({"elevation": "depth"})

    download_estimated_size = 0
    for variable_name in temp_dataset.data_vars:
        coordinates_size = 1.0
        variable = [
            var for var in service.variables if var.short_name == variable_name
        ][0]
        for coordinate_name in temp_dataset.sizes:
            if coordinate_name == "elevation":
                coordinate_name = "depth"
                temp_dataset["elevation"] = temp_dataset.elevation * (-1)
            possible_coordinate_ids = [
                coordinate_names
                for coordinate_names in axis_coordinate_id_mapping.values()
                if coordinate_name in coordinate_names
            ]
            if not possible_coordinate_ids:
                continue
            possible_coordinate_id = possible_coordinate_ids[0]
            coordinates = [
                coord
                for coord in variable.coordinates
                if coord.coordinate_id in possible_coordinate_id
            ]
            if not coordinates:
                continue
            coordinate = coordinates[0]
            chunking_length = coordinate.chunking_length
            if not chunking_length:
                continue
            if coordinate.coordinate_id == "time":
                requested_maximum = (
                    timestamp_or_datestring_to_datetime(
                        temp_dataset[coordinate.coordinate_id].values.max()
                    ).timestamp()
                    * 1e3
                )
                requested_minimum = (
                    timestamp_or_datestring_to_datetime(
                        temp_dataset[coordinate.coordinate_id].values.min()
                    ).timestamp()
                    * 1e3
                )
            else:
                requested_maximum = float(
                    temp_dataset[coordinate.coordinate_id].max().values
                )
                requested_minimum = float(
                    temp_dataset[coordinate.coordinate_id].min().values
                )
            number_of_chunks_needed = get_number_of_chunks_for_coordinate(
                requested_minimum,
                requested_maximum,
                coordinate,
                chunking_length,
            )
            if number_of_chunks_needed is None:
                return None
            coordinates_size *= number_of_chunks_needed * chunking_length
        download_estimated_size += (
            coordinates_size
            * temp_dataset[list(temp_dataset.data_vars)[0]].dtype.itemsize
            / 1048e3
        )

    return download_estimated_size


def get_number_of_chunks_for_coordinate(
    requested_minimum: Optional[float],
    requested_maximum: Optional[float],
    coordinate: CopernicusMarineCoordinate,
    chunking_length: Union[int, float],
) -> Optional[int]:
    maximum_value = coordinate.maximum_value
    minimum_value = coordinate.minimum_value
    # TODO: try to get rid of this type ignores
    # check how bisect works with the time values
    values = coordinate.values
    step_value = coordinate.step
    if not values and (
        maximum_value is not None
        and minimum_value is not None
        and step_value is not None
    ):
        values = [minimum_value]  # type: ignore
        for _ in range(
            0, math.ceil((maximum_value - minimum_value) / step_value)  # type: ignore
        ):
            values.append(values[-1] + step_value)  # type: ignore
    elif not values:
        return None
    elif type(values[0]) is str:
        return None

    values.sort()
    if requested_minimum is None or requested_minimum < values[0]:  # type: ignore
        requested_minimum = values[0]  # type: ignore
    if requested_maximum is None or requested_maximum > values[-1]:  # type: ignore
        requested_maximum = values[-1]  # type: ignore

    index_left_minimum = bisect.bisect_left(values, requested_minimum)  # type: ignore

    if index_left_minimum >= len(
        values
    ):  # we have moved the longitude window and we are at the edge of it
        index_left_minimum = 0
    if index_left_minimum == len(values) - 1 or abs(
        values[index_left_minimum] - requested_minimum  # type: ignore
    ) <= abs(
        values[index_left_minimum + 1] - requested_minimum  # type: ignore
    ):
        chunk_of_requested_minimum = math.floor(
            (index_left_minimum) / chunking_length
        )
    else:
        chunk_of_requested_minimum = math.floor(
            (index_left_minimum + 1) / chunking_length
        )

    index_right_maximum = bisect.bisect_right(values, requested_maximum)  # type: ignore
    index_right_maximum = index_right_maximum - 1
    if (
        index_right_maximum == len(values) - 1
        or index_right_maximum == len(values)
        or abs(values[index_right_maximum] - requested_maximum)  # type: ignore
        <= abs(values[index_right_maximum + 1] - requested_maximum)  # type: ignore
    ):
        chunk_of_requested_maximum = math.floor(
            (index_right_maximum) / chunking_length
        )
    else:
        chunk_of_requested_maximum = math.floor(
            (index_right_maximum + 1) / chunking_length
        )
    return chunk_of_requested_maximum - chunk_of_requested_minimum + 1
