import logging
import pathlib
from datetime import datetime
from typing import Any, Optional, Union

import xarray

from copernicusmarine.core_functions.models import (
    DEFAULT_FILE_EXTENSIONS,
    DatasetChunking,
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
    dataset_chunking: DatasetChunking,
) -> Optional[float]:
    # TODO: Test it not sure how to, maybe ask if the chunk size is correct
    temp_dataset = dataset.copy()
    if "elevation" in dataset.sizes:
        temp_dataset["elevation"] = temp_dataset.elevation * (-1)
        temp_dataset = temp_dataset.rename({"elevation": "depth"})

    download_estimated_size = 0
    for variable_name in temp_dataset.data_vars:
        download_estimated_size += (
            dataset_chunking.get_number_values_variable(str(variable_name))
            * temp_dataset[list(temp_dataset.data_vars)[0]].dtype.itemsize
            / 1048e3
        )

    return download_estimated_size
