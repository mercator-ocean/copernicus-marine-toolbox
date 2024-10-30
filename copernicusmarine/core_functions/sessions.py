import ssl
from typing import Any, List, Literal, Optional, Tuple

import boto3
import botocore
import botocore.config
import certifi
import requests
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
from copernicusmarine.core_functions.utils import create_custom_query_function

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
    if COPERNICUSMARINE_DISABLE_SSL_CONTEXT is not None:
        return None
    return ssl.create_default_context(cafile=certifi.where())


def get_configured_boto3_session(
    endpoint_url: str,
    operation_type: List[Literal["ListObjects", "HeadObject", "GetObject"]],
    username: Optional[str] = None,
    return_ressources: bool = False,
) -> Tuple[Any, Any]:
    config_boto3 = botocore.config.Config(
        s3={"addressing_style": "virtual"},
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
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.trust_env = TRUST_ENV
        self.verify = (
            COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH or certifi.where()
        )
        self.proxies = PROXIES
        if HTTPS_RETRIES:
            self.mount(
                "https://",
                HTTPAdapter(
                    max_retries=Retry(
                        total=HTTPS_RETRIES,
                        backoff_factor=1,
                        status_forcelist=[408, 429, 500, 502, 503, 504],
                    )
                ),
            )

    def request(self, *args, **kwargs):
        kwargs.setdefault("timeout", HTTPS_TIMEOUT)
        return super().request(*args, **kwargs)


def get_configured_requests_session() -> requests.Session:
    return ConfiguredRequestsSession()
