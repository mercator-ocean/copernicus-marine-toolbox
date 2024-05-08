import ssl
from typing import Optional

import aiohttp
import certifi
import nest_asyncio
import requests
import xarray

from copernicusmarine.core_functions.custom_zarr_store import CustomS3Store
from copernicusmarine.core_functions.environment_variables import (
    COPERNICUSMARINE_DISABLE_SSL_CONTEXT,
    COPERNICUSMARINE_TRUST_ENV,
    PROXY_HTTP,
    PROXY_HTTPS,
)
from copernicusmarine.core_functions.utils import parse_access_dataset_url

TRUST_ENV = COPERNICUSMARINE_TRUST_ENV == "True"
PROXIES = {}
if PROXY_HTTP:
    PROXIES["http"] = PROXY_HTTP
if PROXY_HTTPS:
    PROXIES["https"] = PROXY_HTTPS


def _get_ssl_context() -> Optional[ssl.SSLContext]:
    if COPERNICUSMARINE_DISABLE_SSL_CONTEXT is not None:
        return None
    return ssl.create_default_context(cafile=certifi.where())


def get_configured_aiohttp_session() -> aiohttp.ClientSession:
    nest_asyncio.apply()
    connector = aiohttp.TCPConnector(ssl=_get_ssl_context())
    return aiohttp.ClientSession(connector=connector, trust_env=TRUST_ENV)


def get_https_proxy() -> Optional[str]:
    return PROXIES.get("https")


def get_configured_request_session() -> requests.Session:
    session = requests.Session()
    session.trust_env = TRUST_ENV
    session.verify = certifi.where()
    session.proxies = PROXIES
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
        endpoint=endpoint,
        bucket=bucket,
        root_path=root_path,
        copernicus_marine_username=copernicus_marine_username,
    )
    kwargs.update(
        {
            "storage_options": {
                "client_kwargs": {"trust_env": TRUST_ENV, "proxies": PROXIES},
                "ssl": _get_ssl_context(),
            }
        }
    )
    return xarray.open_zarr(store, **kwargs)
