import bisect
import logging
import math
from pathlib import Path
from typing import Any, Optional

import xarray
from pendulum import DateTime

from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineCoordinate,
    CopernicusMarineService,
)
from copernicusmarine.core_functions.models import (
    DEFAULT_FILE_EXTENSIONS,
    DatasetCoordinatesExtent,
    FileFormat,
    GeographicalExtent,
    TimeExtent,
)
from copernicusmarine.core_functions.utils import (
    timestamp_or_datestring_to_datetime,
)
from copernicusmarine.download_functions.subset_xarray import COORDINATES_LABEL

logger = logging.getLogger("copernicusmarine")


def get_file_extension(file_format: FileFormat) -> str:
    if file_format == "zarr":
        return ".zarr"
    else:
        return ".nc"


def get_filename(
    filename: Optional[str],
    dataset: xarray.Dataset,
    dataset_id: str,
    file_format: FileFormat,
) -> str:
    if filename:
        if Path(filename).suffix in DEFAULT_FILE_EXTENSIONS:
            return filename
        else:
            return filename + get_file_extension(file_format)
    else:
        return _build_filename_from_dataset(dataset, dataset_id, file_format)


def _build_filename_from_dataset(
    dataset: xarray.Dataset,
    dataset_id: str,
    file_format: FileFormat,
) -> str:
    dataset_variables = "-".join(list(dataset.keys()))
    variables = (
        "multi-vars"
        if (len(dataset_variables) > 15 and len(list(dataset.keys())) > 1)
        else dataset_variables
    )
    longitudes = _format_longitudes(
        _get_min_coordinate(dataset, "longitude"),
        _get_max_coordinate(dataset, "longitude"),
    )
    latitudes = _format_latitudes(
        _get_min_coordinate(dataset, "latitude"),
        _get_max_coordinate(dataset, "latitude"),
    )
    depths = _format_depths(
        _get_min_coordinate(dataset, "depth"),
        _get_max_coordinate(dataset, "depth"),
    )

    min_time_coordinate = _get_min_coordinate(dataset, "time")
    max_time_coordinate = _get_max_coordinate(dataset, "time")
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


def _get_min_coordinate(dataset: xarray.Dataset, coordinate: str) -> Any:
    for coord_label in COORDINATES_LABEL[coordinate]:
        if coord_label in dataset.sizes:
            return min(dataset[coord_label].values)
    return None


def _get_max_coordinate(dataset: xarray.Dataset, coordinate: str) -> Any:
    for coord_label in COORDINATES_LABEL[coordinate]:
        if coord_label in dataset.sizes:
            return max(dataset[coord_label].values)
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
    minimum_datetime: Optional[DateTime], maximum_datetime: Optional[DateTime]
) -> str:
    if minimum_datetime is None or maximum_datetime is None:
        return ""
    else:
        if minimum_datetime == maximum_datetime:
            formatted_datetime = f"{minimum_datetime.format('YYYY-MM-DD')}"
        else:
            formatted_datetime = (
                f"{minimum_datetime.format('YYYY-MM-DD')}-"
                f"{maximum_datetime.format('YYYY-MM-DD')}"
            )
        return formatted_datetime


def get_dataset_coordinates_extent(
    dataset: xarray.Dataset,
) -> DatasetCoordinatesExtent:
    minimum_time = _get_min_coordinate(dataset, "time")
    if minimum_time:
        minimum_time = timestamp_or_datestring_to_datetime(
            minimum_time
        ).to_iso8601_string()
    maximum_time = _get_max_coordinate(dataset, "time")
    if maximum_time:
        maximum_time = timestamp_or_datestring_to_datetime(
            maximum_time
        ).to_iso8601_string()
    coordinates_extent = DatasetCoordinatesExtent(
        longitude=GeographicalExtent(
            minimum=_get_min_coordinate(dataset, "longitude"),
            maximum=_get_max_coordinate(dataset, "longitude"),
        ),
        latitude=GeographicalExtent(
            minimum=_get_min_coordinate(dataset, "latitude"),
            maximum=_get_max_coordinate(dataset, "latitude"),
        ),
        time=TimeExtent(
            minimum=minimum_time,
            maximum=maximum_time,
        ),
    )
    if "depth" in dataset.sizes:
        coordinates_extent.depth = GeographicalExtent(
            minimum=_get_min_coordinate(dataset, "depth"),
            maximum=_get_max_coordinate(dataset, "depth"),
        )
    elif "elevation" in dataset.sizes:
        coordinates_extent.elevation = GeographicalExtent(
            minimum=_get_min_coordinate(dataset, "depth"),
            maximum=_get_max_coordinate(dataset, "depth"),
        )
    return coordinates_extent


def get_message_formatted_dataset_size_estimation(
    estimation_size_final_result: Optional[float],
    estimation_data_downloaded: Optional[float],
) -> str:
    return (
        f"Estimated size of the dataset file is "
        f"{estimation_size_final_result:.3f} MB"
        f"\nEstimated size of the data that needs "
        f"to be downloaded to obtain the result:"
        f" {estimation_data_downloaded:.0f} MB"
        "\nThis is a very rough estimate that is"
        " generally higher than the actual size of the"
        "  data that needs to be downloaded."
    )


def get_approximation_size_final_result(
    dataset: xarray.Dataset,
) -> Optional[float]:
    coordinates_size = 1
    for coordinate_name in dataset.sizes:
        coordinates_size *= dataset[coordinate_name].size
    estimate_size = (
        coordinates_size
        * len(list(dataset.data_vars))
        * dataset[list(dataset.data_vars)[0]].dtype.itemsize
        / 1048e3
    )
    return estimate_size


def get_approximation_size_data_downloaded(
    dataset: xarray.Dataset, service: CopernicusMarineService
) -> Optional[float]:
    temp_dataset = dataset.copy()
    if "elevation" in dataset.sizes:
        temp_dataset["elevation"] = temp_dataset.elevation * (-1)
        temp_dataset = temp_dataset.rename({"elevation": "depth"})

    download_estimated_size = 0
    for variable_name in temp_dataset.data_vars:
        coordinates_size = 1
        variable = [
            var for var in service.variables if var.short_name == variable_name
        ][0]
        for coordinate_name in temp_dataset.sizes:
            if coordinate_name == "elevation":
                coordinate_name = "depth"
                temp_dataset["elevation"] = temp_dataset.elevation * (-1)
            possible_coordinate_id = [
                coordinate_names
                for coordinate_names in COORDINATES_LABEL.values()
                if coordinate_name in coordinate_names
            ][0]
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
            number_of_chunks_needed = get_number_of_chunks_for_coordinate(
                temp_dataset, coordinate, chunking_length
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
    dataset: xarray.Dataset,
    coordinate: CopernicusMarineCoordinate,
    chunking_length: int,
) -> Optional[int]:
    maximum_value = coordinate.maximum_value
    minimum_value = coordinate.minimum_value
    values = coordinate.values
    step_value = coordinate.step
    if not values and (
        maximum_value is not None
        and minimum_value is not None
        and step_value is not None
    ):
        values = [minimum_value]
        for _ in range(
            0, math.ceil((maximum_value - minimum_value) / step_value)
        ):
            values.append(values[-1] + step_value)
    elif not values:
        return None

    if coordinate.coordinate_id == "time":
        requested_maximum = (
            timestamp_or_datestring_to_datetime(
                dataset[coordinate.coordinate_id].values.max()
            ).timestamp()
            * 1e3
        )
        requested_minimum = (
            timestamp_or_datestring_to_datetime(
                dataset[coordinate.coordinate_id].values.min()
            ).timestamp()
            * 1e3
        )
    else:
        requested_maximum = float(
            dataset[coordinate.coordinate_id].max().values
        )
        requested_minimum = float(
            dataset[coordinate.coordinate_id].min().values
        )

    values.sort()
    index_left = bisect.bisect_left(values, requested_minimum)
    if index_left == len(values) - 1:
        chunk_of_requested_minimum = math.floor((index_left) / chunking_length)
    elif abs(values[index_left] - requested_minimum) <= abs(
        values[index_left + 1] - requested_minimum
    ):
        chunk_of_requested_minimum = math.floor(index_left / chunking_length)
    else:
        chunk_of_requested_minimum = math.floor(
            (index_left + 1) / chunking_length
        )

    index_left = bisect.bisect_left(values, requested_maximum)
    if index_left == len(values) - 1 or index_left == len(values):
        chunk_of_requested_maximum = math.floor((index_left) / chunking_length)
    elif abs(values[index_left] - requested_maximum) <= abs(
        values[index_left + 1] - requested_maximum
    ):
        chunk_of_requested_maximum = math.floor(index_left / chunking_length)
    else:
        chunk_of_requested_maximum = math.floor(
            (index_left + 1) / chunking_length
        )
    return chunk_of_requested_maximum - chunk_of_requested_minimum + 1


def early_exit_message(message: str) -> str:
    return (
        message
        + "\n Couldn't compute the approximation "
        + "of the downloaded data size."
    )
