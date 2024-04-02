import logging
import pathlib
from typing import Optional

import xarray
import zarr
from dask.delayed import Delayed
from tqdm.dask import TqdmCallback

from copernicusmarine.core_functions.exceptions import (
    NetCDFCompressionNotAvailable,
)

logger = logging.getLogger("copernicus_marine_root_logger")


def get_delayed_download(
    dataset: xarray.Dataset,
    output_path: pathlib.Path,
    netcdf_compression_enabled: bool,
    netcdf_compression_level: Optional[int],
    netcdf3_compatible: bool,
):
    if output_path.suffix == ".zarr":
        if netcdf_compression_enabled:
            raise NetCDFCompressionNotAvailable(
                "--netcdf-compression-enabled option cannot be used when "
                "writing to ZARR"
            )
        delayed = _prepare_download_dataset_as_zarr(dataset, output_path)
    else:
        delayed = _prepare_download_dataset_as_netcdf(
            dataset,
            output_path,
            netcdf_compression_enabled,
            netcdf_compression_level,
            netcdf3_compatible,
        )
    return delayed


def download_delayed_dataset(
    delayed: Delayed, disable_progress_bar: bool
) -> None:
    if disable_progress_bar:
        delayed.compute()
    else:
        with TqdmCallback():
            delayed.compute()


def _prepare_download_dataset_as_netcdf(
    dataset: xarray.Dataset,
    output_path: pathlib.Path,
    netcdf_compression_enabled: bool,
    netcdf_compression_level: Optional[int],
    netcdf3_compatible: bool,
):
    logger.debug("Writing dataset to NetCDF")
    if netcdf_compression_enabled:
        complevel = (
            1 if netcdf_compression_level is None else netcdf_compression_level
        )
        logger.info(f"NetCDF compression enabled with level {complevel}")
        comp = dict(
            zlib=True, complevel=complevel, contiguous=False, shuffle=True
        )
        encoding = {var: comp for var in dataset.data_vars}
    else:
        encoding = None
    xarray_download_format = "NETCDF3_CLASSIC" if netcdf3_compatible else None
    return dataset.to_netcdf(
        output_path,
        mode="w",
        compute=False,
        encoding=encoding,
        format=xarray_download_format,
    )


def _prepare_download_dataset_as_zarr(
    dataset: xarray.Dataset, output_path: pathlib.Path
):
    logger.debug("Writing dataset to Zarr")
    store = zarr.DirectoryStore(output_path)
    return dataset.to_zarr(store=store, mode="w", compute=False)
