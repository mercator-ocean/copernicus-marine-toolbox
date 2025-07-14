import logging
import ssl
from typing import Any, Literal, Optional

import boto3
import botocore
import botocore.config
import certifi
import requests
import requests.auth
from requests.adapters import HTTPAdapter, Retry

from copernicusmarine.core_functions.environment_variables import (
    COPERNICUSMARINE_DISABLE_SSL_CONTEXT,
    COPERNICUSMARINE_HTTPS_RETRIES,
    COPERNICUSMARINE_HTTPS_TIMEOUT,
    COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH,
    COPERNICUSMARINE_TRUST_ENV,
    PROXY_HTTP,
    PROXY_HTTPS,
)
from copernicusmarine.core_functions.utils import (
    construct_query_params_for_marine_data_store_monitoring,
    create_custom_query_function,
)

logger = logging.getLogger("copernicusmarine")

TRUST_ENV = COPERNICUSMARINE_TRUST_ENV == "True"
PROXIES = {}
if PROXY_HTTP:
    PROXIES["http"] = PROXY_HTTP
if PROXY_HTTPS:
    PROXIES["https"] = PROXY_HTTPS
try:
    HTTPS_TIMEOUT = float(COPERNICUSMARINE_HTTPS_TIMEOUT)
except ValueError:
    HTTPS_TIMEOUT = 60
try:
    HTTPS_RETRIES = int(COPERNICUSMARINE_HTTPS_RETRIES)
except ValueError:
    HTTPS_RETRIES = 5


def get_ssl_context() -> Optional[ssl.SSLContext]:
    if COPERNICUSMARINE_DISABLE_SSL_CONTEXT == "True":
        return None
    if COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH:
        return ssl.create_default_context(
            capath=COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH
        )
    return ssl.create_default_context(cafile=certifi.where())


def get_configured_boto3_session(
    endpoint_url: str,
    operation_type: list[Literal["ListObjectsV2", "HeadObject", "GetObject"]],
    username: Optional[str] = None,
    return_ressources: bool = False,
) -> tuple[Any, Any]:
    config_boto3 = botocore.config.Config(
        signature_version=botocore.UNSIGNED,
        retries={"max_attempts": 10, "mode": "standard"},
    )
    s3_session = boto3.Session()
    s3_client = s3_session.client(
        "s3",
        config=config_boto3,
        endpoint_url=endpoint_url,
    )
    for operation in operation_type:
        # Register the botocore event handler for adding custom query params
        # to S3 HEAD and GET requests
        s3_client.meta.events.register(
            f"before-call.s3.{operation}",
            create_custom_query_function(username),
        )
    if not return_ressources:
        return s3_client, None
    s3_resource = boto3.resource(
        "s3",
        config=config_boto3,
        endpoint_url=endpoint_url,
    )
    return s3_client, s3_resource


# TODO: add tests
# example: with https://httpbin.org/delay/10 or
# https://medium.com/@mpuig/testing-robust-requests-with-python-a06537d97771
class ConfiguredRequestsSession(requests.Session):
    def __init__(
        self,
        *args,
        timeout: float = HTTPS_TIMEOUT,
        retries: int = HTTPS_RETRIES,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.trust_env = TRUST_ENV
        if COPERNICUSMARINE_DISABLE_SSL_CONTEXT == "True":
            self.verify = False
        else:
            self.verify = (
                COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH or certifi.where()
            )
        self.proxies = PROXIES
        if retries:
            self.mount(
                "https://",
                HTTPAdapter(
                    max_retries=Retry(
                        total=retries,
                        backoff_factor=1,
                        status_forcelist=[
                            408,
                            429,
                            500,
                            502,
                            503,
                            504,
                        ],
                        allowed_methods=False,
                    )
                ),
            )
        self.timeout = timeout

    def request(self, *args, **kwargs):
        kwargs.setdefault("timeout", self.timeout)
        return super().request(*args, **kwargs)


def get_configured_requests_session(
    timeout: float = HTTPS_TIMEOUT, retries: int = HTTPS_RETRIES
) -> requests.Session:
    return ConfiguredRequestsSession(timeout=timeout, retries=retries)


# from https://stackoverflow.com/questions/29931671/making-an-api-call-in-python-with-an-api-that-requires-a-bearer-token # noqa
class BearerAuth(requests.auth.AuthBase):
    """
    Allow to pass the bearer as "auth" argument to the requests.get method and not as a header.

    Hence the headers are not overwritten by the netrc file as stated here:
    https://requests.readthedocs.io/en/latest/user/authentication/#netrc-authentication
    """  # noqa

    def __init__(self, token):
        self.token = token

    def __call__(self, r):
        r.headers["authorization"] = "Bearer " + self.token
        return r


class JsonParserConnection:
    def __init__(
        self, timeout: float = HTTPS_TIMEOUT, retries: int = HTTPS_RETRIES
    ) -> None:
        self.session = get_configured_requests_session(timeout, retries)

    def get_json_file(self, url: str) -> dict[str, Any]:
        logger.debug(f"Fetching json file at this url: {url}")
        with self.session.get(
            url,
            params=construct_query_params_for_marine_data_store_monitoring(),
            proxies=self.session.proxies,
        ) as response:
            response.raise_for_status()
            return response.json()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session.close()
