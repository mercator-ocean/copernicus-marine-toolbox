import logging
import math
import pathlib
from datetime import datetime
from typing import Any

import numpy
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
    filename: str | None,
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
            None,
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
    time_format: str | None = None,
) -> str:
    """
    In the sparse dataset case we don't have the dataset to build the filename from.
    Also, used for the split-on where we need more precise timestamp handling.
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
            request.start_datetime, request.end_datetime, time_format
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
    minimum_longitude: float | None, maximum_longitude: float | None
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
    minimum_latitude: float | None, maximum_latitude: float | None
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
    minimum_value: float | None,
    maximum_value: float | None,
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
    minimum_depth: float | None, maximum_depth: float | None
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
    minimum_datetime: datetime | None,
    maximum_datetime: datetime | None,
    time_format: str | None,
) -> str:
    time_format = time_format or "%Y-%m-%d"
    if minimum_datetime is None or maximum_datetime is None:
        return ""
    else:
        if minimum_datetime.strftime(time_format) == maximum_datetime.strftime(
            time_format
        ):
            formatted_datetime = f"{minimum_datetime.strftime(time_format)}"
        else:
            formatted_datetime = (
                f"{minimum_datetime.strftime(time_format)}-"
                f"{maximum_datetime.strftime(time_format)}"
            )
        return formatted_datetime


def get_dataset_coordinates_extent(
    dataset: xarray.Dataset, axis_coordinate_id_mapping: dict[str, str]
) -> list[GeographicalExtent | TimeExtent]:
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
) -> GeographicalExtent | TimeExtent | None:
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
    dataset: xarray.Dataset,
    axis_coordinate_id_mapping: dict[str, str],
    file_format: FileFormat = "netcdf",
) -> float:
    baseline_size = 0.013
    total_values_size_bytes = 0.0
    coordinates_size = 0

    for variable_name in dataset.data_vars:
        variable = dataset[variable_name]
        if file_format == "zarr":
            total_values_size_bytes += _estimate_zarr_variable_size_bytes(
                variable
            )
        else:
            storage_dtype = variable.encoding.get("dtype", variable.dtype)
            item_size = numpy.dtype(storage_dtype).itemsize
            total_values_size_bytes += variable.size * item_size

    for coordinate_name in axis_coordinate_id_mapping.values():
        if coordinate_name in dataset.sizes:
            coordinates_size += dataset[coordinate_name].size

    coordinate_overhead = coordinates_size * numpy.dtype("float64").itemsize
    if file_format == "zarr":
        # Zarr containers include metadata files and chunk key/index overhead.
        # Keep a small fixed overhead to avoid underestimation for tiny outputs.
        coordinate_overhead += 8 * 1024
    estimate_size = (
        baseline_size
        + (total_values_size_bytes + coordinate_overhead) / 1048e3
    )

    return estimate_size


def _estimate_zarr_variable_size_bytes(variable: xarray.DataArray) -> float:
    storage_dtype = variable.encoding.get("dtype", variable.dtype)
    item_size = numpy.dtype(storage_dtype).itemsize
    uncompressed_bytes = variable.size * item_size

    chunk_shape = _get_zarr_chunk_shape(variable)
    chunk_count = 1
    if chunk_shape:
        chunk_count = math.prod(
            math.ceil(dim_size / chunk_size)
            for dim_size, chunk_size in zip(variable.shape, chunk_shape)
        )

    compression_ratio = _estimate_zarr_compression_ratio(
        variable,
        chunk_count,
    )

    # Per-chunk key/index and per-array metadata overhead.
    overhead_bytes = chunk_count * 180 + 2048
    return uncompressed_bytes * compression_ratio + overhead_bytes


def _get_zarr_chunk_shape(variable: xarray.DataArray) -> tuple[int, ...]:
    if variable.chunksizes:
        dask_chunk_shape = []
        for dim_name, dim_size in zip(variable.dims, variable.shape):
            dim_chunks = variable.chunksizes.get(dim_name)
            if dim_chunks:
                dask_chunk_shape.append(int(max(1, dim_chunks[0])))
            else:
                dask_chunk_shape.append(int(max(1, dim_size)))
        return tuple(dask_chunk_shape)

    encoding_chunks = variable.encoding.get("chunks")
    if isinstance(encoding_chunks, tuple) and encoding_chunks:
        return tuple(int(max(1, chunk)) for chunk in encoding_chunks)
    if isinstance(encoding_chunks, list) and encoding_chunks:
        return tuple(int(max(1, chunk)) for chunk in encoding_chunks)

    preferred_chunks = variable.encoding.get("preferred_chunks")
    if isinstance(preferred_chunks, dict):
        preferred_chunk_shape = []
        for dim_name, dim_size in zip(variable.dims, variable.shape):
            chunk_size = preferred_chunks.get(dim_name, dim_size)
            preferred_chunk_shape.append(int(max(1, chunk_size)))
        return tuple(preferred_chunk_shape)

    return tuple(int(max(1, dim_size)) for dim_size in variable.shape)


def _estimate_zarr_compression_ratio(
    variable: xarray.DataArray,
    chunk_count: int,
) -> float:
    compressor = None
    compressors = variable.encoding.get("compressors")
    if compressors and (isinstance(compressors, (tuple, list))):
        compressor = compressors[0]
    else:
        compressor = variable.encoding.get("compressor")

    if compressor is None:
        return 1.0

    compressor_name = (
        getattr(compressor, "cname", None) or compressor.__class__.__name__
    ).lower()
    clevel = getattr(compressor, "clevel", None)
    compression_level: float = (
        clevel if clevel is not None else getattr(compressor, "level", 5)
    )

    ratio = 0.7
    if "lz4" in compressor_name:
        ratio = 0.74
    elif "zstd" in compressor_name:
        ratio = 0.52
    elif "zlib" in compressor_name or "gzip" in compressor_name:
        ratio = 0.58
    elif "blosclz" in compressor_name:
        ratio = 0.66

    ratio -= min(0.16, float(compression_level) * 0.015)

    storage_dtype = variable.encoding.get("dtype", variable.dtype)
    dtype_kind = numpy.dtype(storage_dtype).kind
    shuffle = getattr(compressor, "shuffle", None)
    if dtype_kind in ("i", "u") and shuffle is not None:
        if str(shuffle).upper() not in {"NOSHUFFLE", "0", "NONE"}:
            ratio -= 0.04

    if (
        "scale_factor" in variable.encoding
        or "add_offset" in variable.encoding
    ):
        ratio -= 0.03

    # Large numbers of chunks often correlate with stronger effective
    # compression in subset outputs (more sparse/repetitive chunk regions).
    chunk_factor = min(1.0, 1.25 / (max(1, chunk_count) ** 0.4))
    ratio *= chunk_factor

    return min(max(ratio, 0.08), 1.0)


def get_approximation_size_data_downloaded(
    dataset: xarray.Dataset,
    dataset_chunking: DatasetChunking,
) -> float | None:
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


def get_approximation_size_final_result_csv(
    dataset: xarray.Dataset,
) -> numpy.float64:
    n_rows = numpy.prod([dataset.sizes[dim] for dim in dataset.dims])
    n_columns = len(dataset.data_vars) + len(dataset.dims)

    if "depth" in dataset.sizes or "elevation" in dataset.sizes:
        bytes_per_value = 10
    else:
        bytes_per_value = 7

    csv_size_bytes = n_rows * n_columns * bytes_per_value
    csv_size_mb = csv_size_bytes / (1024**2)
    return csv_size_mb
