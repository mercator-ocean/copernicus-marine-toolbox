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


def get_ssl_context() -> Optional[ssl.SSLContext]:
    if COPERNICUSMARINE_DISABLE_SSL_CONTEXT is not None:
        return None
    return ssl.create_default_context(cafile=certifi.where())


def get_https_proxy() -> Optional[str]:
    return PROXIES.get("https")


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


def get_configured_requests_session() -> requests.Session:
    retry_http_codes = [
        408,  # Request Timeout
        429,  # Too Many Requests
        500,  # Internal Server Error
        502,  # Bad Gateway
        503,  # Service Unavailable
        504,  # Gateway Timeout
    ]
    session = requests.Session()
    session.trust_env = TRUST_ENV
    session.verify = (
        COPERNICUSMARINE_SET_SSL_CERTIFICATE_PATH or certifi.where()
    )
    session.proxies = PROXIES
    session.mount(
        "https://",
        HTTPAdapter(
            max_retries=Retry(
                total=5,
                backoff_factor=1,
                status_forcelist=retry_http_codes,
            )
        ),
    )
    return session
