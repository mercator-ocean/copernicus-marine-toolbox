import os
import ssl
from typing import Optional

import aiohttp
import certifi
import nest_asyncio
import requests
import xarray

from copernicusmarine.core_functions.custom_zarr_store import CustomS3Store
from copernicusmarine.core_functions.utils import (
    construct_query_params_for_marine_data_store_monitoring,
    parse_access_dataset_url,
)

TRUST_ENV = True


def _get_ssl_context() -> Optional[ssl.SSLContext]:
    if os.getenv("COPERNICUSMARINE_DISABLE_SSL_CONTEXT") is not None:
        return None
    return ssl.create_default_context(cafile=certifi.where())


def get_configured_aiohttp_session() -> aiohttp.ClientSession:
    nest_asyncio.apply()
    connector = aiohttp.TCPConnector(ssl=_get_ssl_context())
    return aiohttp.ClientSession(connector=connector, trust_env=TRUST_ENV)


def get_configured_request_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = TRUST_ENV
    session.verify = certifi.where()
    return session


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
    store = CustomS3Store(
        endpoint=endpoint, bucket=bucket, root_path=root_path
    )
    kwargs.update(
        {
            "storage_options": {
                "params": construct_query_params_for_marine_data_store_monitoring(
                    username=copernicus_marine_username
                ),
                "client_kwargs": {"trust_env": TRUST_ENV},
                "ssl": _get_ssl_context(),
            }
        }
    )
    return xarray.open_zarr(store, **kwargs)
