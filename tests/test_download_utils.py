import numpy
import xarray

from copernicusmarine.download_functions.utils import (
    _estimate_zarr_compression_ratio,
    _get_zarr_chunk_shape,
    get_approximation_size_final_result,
)


class _FakeCompressor:
    def __init__(self, cname: str, clevel: int, shuffle: str):
        self.cname = cname
        self.clevel = clevel
        self.shuffle = shuffle


def _build_test_dataset(shape: tuple[int, int]) -> xarray.Dataset:
    data = numpy.ones(shape, dtype=numpy.float32)
    variable = xarray.DataArray(data=data, dims=("latitude", "longitude"))
    variable.encoding["dtype"] = numpy.dtype("int16")
    variable.encoding["scale_factor"] = 0.001
    variable.encoding["add_offset"] = 10.0
    variable.encoding["chunks"] = (25, 25)
    variable.encoding["compressors"] = (
        _FakeCompressor("lz4", clevel=5, shuffle="SHUFFLE"),
    )
    return xarray.Dataset({"thetao": variable})


def test_zarr_estimate_is_smaller_than_netcdf_for_compressed_encoding() -> None:
    dataset = _build_test_dataset((200, 200))
    axis_mapping = {"x": "longitude", "y": "latitude"}

    netcdf_estimate = get_approximation_size_final_result(
        dataset,
        axis_mapping,
        file_format="netcdf",
    )
    zarr_estimate = get_approximation_size_final_result(
        dataset,
        axis_mapping,
        file_format="zarr",
    )

    assert zarr_estimate > 0
    assert zarr_estimate < netcdf_estimate


def test_zarr_compression_ratio_decreases_with_chunk_count() -> None:
    dataset = _build_test_dataset((100, 100))
    variable = dataset["thetao"]

    ratio_few_chunks = _estimate_zarr_compression_ratio(
        variable,
        chunk_count=4,
    )
    ratio_many_chunks = _estimate_zarr_compression_ratio(
        variable,
        chunk_count=256,
    )

    assert ratio_many_chunks < ratio_few_chunks


def test_get_zarr_chunk_shape_prefers_dask_chunksizes() -> None:
    dask_array = xarray.DataArray(
        data=numpy.ones((40, 30), dtype=numpy.float32),
        dims=("latitude", "longitude"),
    ).chunk({"latitude": 10, "longitude": 15})
    dask_array.encoding["chunks"] = (40, 30)

    assert _get_zarr_chunk_shape(dask_array) == (10, 15)
