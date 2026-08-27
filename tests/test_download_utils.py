import numpy
import pytest
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


@pytest.mark.parametrize(
    "compressor_name,expected_base",
    [
        ("lz4", 0.74),
        ("zstd", 0.52),
        ("zlib", 0.58),
        ("gzip", 0.58),
        ("blosclz", 0.66),
    ],
)
def test_zarr_compression_ratio_respects_compressor_family(
    compressor_name: str,
    expected_base: float,
) -> None:
    dataset = _build_test_dataset((100, 100))
    variable = dataset["thetao"]
    variable.encoding["compressors"] = (
        _FakeCompressor(compressor_name, clevel=0, shuffle="NOSHUFFLE"),
    )
    variable.encoding.pop("scale_factor", None)
    variable.encoding.pop("add_offset", None)

    ratio = _estimate_zarr_compression_ratio(variable, chunk_count=1)

    assert pytest.approx(ratio, rel=1e-6) == expected_base


def test_zarr_compression_ratio_uses_level_scale_and_offset_and_shuffle() -> None:
    dataset = _build_test_dataset((100, 100))
    variable = dataset["thetao"]

    stronger = _estimate_zarr_compression_ratio(variable, chunk_count=1)

    variable.encoding["compressors"] = (
        _FakeCompressor("lz4", clevel=0, shuffle="NOSHUFFLE"),
    )
    variable.encoding.pop("scale_factor", None)
    variable.encoding.pop("add_offset", None)
    weaker = _estimate_zarr_compression_ratio(variable, chunk_count=1)

    assert stronger < weaker


def test_zarr_compression_ratio_returns_one_when_no_compressor() -> None:
    dataset = _build_test_dataset((100, 100))
    variable = dataset["thetao"]
    variable.encoding.pop("compressors", None)
    variable.encoding.pop("compressor", None)

    assert _estimate_zarr_compression_ratio(variable, chunk_count=1) == 1.0


def test_zarr_compression_ratio_is_clamped_to_bounds() -> None:
    dataset = _build_test_dataset((100, 100))
    variable = dataset["thetao"]

    variable.encoding["compressors"] = (
        _FakeCompressor("zstd", clevel=9, shuffle="SHUFFLE"),
    )
    very_small = _estimate_zarr_compression_ratio(
        variable, chunk_count=10**9
    )

    variable.encoding["compressors"] = (
        _FakeCompressor("unknown", clevel=0, shuffle="NOSHUFFLE"),
    )
    variable.encoding.pop("scale_factor", None)
    variable.encoding.pop("add_offset", None)
    very_large = _estimate_zarr_compression_ratio(variable, chunk_count=1)

    assert very_small >= 0.08
    assert very_large <= 1.0


def test_get_zarr_chunk_shape_uses_encoding_chunks_when_not_dask_chunked() -> None:
    data_array = xarray.DataArray(
        data=numpy.ones((12, 10), dtype=numpy.float32),
        dims=("latitude", "longitude"),
    )
    data_array.encoding["chunks"] = (4, 5)

    assert _get_zarr_chunk_shape(data_array) == (4, 5)


def test_get_zarr_chunk_shape_uses_preferred_chunks_as_fallback() -> None:
    data_array = xarray.DataArray(
        data=numpy.ones((12, 10), dtype=numpy.float32),
        dims=("latitude", "longitude"),
    )
    data_array.encoding["preferred_chunks"] = {
        "latitude": 3,
        "longitude": 2,
    }

    assert _get_zarr_chunk_shape(data_array) == (3, 2)


def test_get_zarr_chunk_shape_defaults_to_full_shape_without_metadata() -> None:
    data_array = xarray.DataArray(
        data=numpy.ones((12, 10), dtype=numpy.float32),
        dims=("latitude", "longitude"),
    )

    assert _get_zarr_chunk_shape(data_array) == (12, 10)
