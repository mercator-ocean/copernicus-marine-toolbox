import logging
from typing import Optional

import xarray
import zarr

from copernicusmarine.core_functions.utils import parse_access_dataset_url

logger = logging.getLogger("copernicusmarine")


def open_zarr(
    dataset_url: str,
    copernicus_marine_username: Optional[str] = None,
    **kwargs,
) -> xarray.Dataset:
    (
        endpoint,
        bucket,
        root_path,
    ) = parse_access_dataset_url(dataset_url)
    if zarr.__version__.startswith("2"):
        from copernicusmarine.core_functions.custom_s3_store_zarr_v2 import (
            CustomS3StoreZarrV2,
        )

        logger.debug("Using custom store for Zarr Python library v2")
        store = CustomS3StoreZarrV2(
            endpoint=endpoint,
            bucket=bucket,
            root_path=root_path,
            copernicus_marine_username=copernicus_marine_username,
        )
        return xarray.open_zarr(store, **kwargs)
    else:
        from copernicusmarine.core_functions.custom_s3_store_zarr_v3 import (
            CustomS3StoreZarrV3,
        )

        logger.debug("Using custom store for Zarr Python library v3")
        store = CustomS3StoreZarrV3(
            endpoint=endpoint,
            bucket=bucket,
            root_path=root_path,
            copernicus_marine_username=copernicus_marine_username,
            read_only=True,
        )
        return xarray.open_zarr(store, **kwargs, zarr_format=2)
