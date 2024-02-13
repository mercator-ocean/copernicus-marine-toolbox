import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import xarray
from pandas import Timestamp

from copernicusmarine.core_functions.models import (
    DEFAULT_FILE_EXTENSIONS,
    FileFormat,
)
from copernicusmarine.download_functions.subset_xarray import COORDINATES_LABEL

logger = logging.getLogger("copernicus_marine_root_logger")


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
        Timestamp(min_time_coordinate).to_pydatetime()
        if min_time_coordinate is not None
        else None,
        Timestamp(max_time_coordinate).to_pydatetime()
        if max_time_coordinate is not None
        else None,
    )

    filename = "_".join(
        filter(
            None,
            [dataset_id, variables, longitudes, latitudes, depths, datetimes],
        )
    )
    filename = filename if len(filename) < 250 else filename[250:]

    return filename + get_file_extension(file_format)


def _get_min_coordinate(dataset: xarray.Dataset, coordinate: str):
    for coord_label in COORDINATES_LABEL[coordinate]:
        if coord_label in dataset.dims:
            return min(dataset[coord_label].values)
    return None


def _get_max_coordinate(dataset: xarray.Dataset, coordinate: str):
    for coord_label in COORDINATES_LABEL[coordinate]:
        if coord_label in dataset.dims:
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


def get_formatted_dataset_size_estimation(dataset: xarray.Dataset) -> str:
    coordinates_size = 1
    for coordinate in dataset.dims:
        coordinates_size *= dataset[coordinate].size
    estimate_size = (
        coordinates_size
        * len(list(dataset.data_vars))
        * dataset[list(dataset.data_vars)[0]].dtype.itemsize
        / 1048e3
    )
    return f"{estimate_size:.3f} MB"
