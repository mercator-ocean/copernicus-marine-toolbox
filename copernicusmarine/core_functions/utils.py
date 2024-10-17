import concurrent.futures
import functools
import logging
import pathlib
import re
from importlib.metadata import version
from typing import (
    Any,
    Callable,
    Iterable,
    Iterator,
    Literal,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import cftime
import numpy
import pandas as pd
import pendulum
import pendulum.exceptions
import xarray
from pendulum import DateTime
from requests import PreparedRequest
from tqdm import tqdm

from copernicusmarine import __version__ as copernicusmarine_version
from copernicusmarine.core_functions.exceptions import WrongDatetimeFormat

logger = logging.getLogger("copernicusmarine")


FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE = "Do you want to proceed with download?"


DATETIME_SUPPORTED_FORMATS = [
    "%Y",
    "%Y-%m-%d",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S.%fZ",
    "%Y-%m-%d %H:%M:%S.%f%Z",
]


def get_unique_filename(
    filepath: pathlib.Path, overwrite_option: bool
) -> pathlib.Path:
    if not overwrite_option:
        parent = filepath.parent
        filename = filepath.stem
        extension = filepath.suffix
        counter = 1

        while filepath.exists():
            filepath = parent / (
                filename + "_(" + str(counter) + ")" + extension
            )
            counter += 1

    return filepath


_T = TypeVar("_T")
_S = TypeVar("_S")


def map_reject_none(
    function: Callable[[_S], Optional[_T]], iterable: Iterable[_S]
) -> Iterable[_T]:
    return (element for element in map(function, iterable) if element)


def next_or_raise_exception(
    iterator: Iterator[_T], exception_to_raise: Exception
) -> _T:
    try:
        return next(iterator)
    except Exception as exception:
        raise exception_to_raise from exception


def construct_url_with_query_params(url, query_params: dict) -> Optional[str]:
    req = PreparedRequest()
    req.prepare_url(url, query_params)
    return req.url


def construct_query_params_for_marine_data_store_monitoring(
    username: Optional[str] = None,
) -> dict:
    query_params = {
        "x-cop-client": "copernicus-marine-toolbox",
        "x-cop-client-version": copernicusmarine_version,
    }
    if username:
        query_params["x-cop-user"] = username
    return query_params


def datetime_parser(date: Union[str, numpy.datetime64]) -> DateTime:
    if date == "now":
        return pendulum.now(tz="UTC")
    try:
        if isinstance(date, numpy.datetime64):
            date = str(date)
        parsed_datetime = pendulum.parse(date)
        # ignoring types because one needs to pass
        # `exact=True` to `parse` method to get
        # something else than `pendulum.DateTime`
        return parsed_datetime  # type: ignore
    except pendulum.exceptions.ParserError:
        pass
    raise WrongDatetimeFormat(date)


def timestamp_parser(
    timestamp: Union[int, float], unit: Literal["s", "ms"] = "ms"
) -> DateTime:
    """
    Convert a timestamp in milliseconds to a pendulum DateTime object
    by default. The unit can be changed to seconds by passing "s" as
    the unit.
    """
    conversion_factor = 1 if unit == "s" else 1e3
    return pendulum.from_timestamp(timestamp / conversion_factor, tz="UTC")


def timestamp_or_datestring_to_datetime(
    date: Union[str, int, float, numpy.datetime64]
) -> DateTime:
    if isinstance(date, int) or isinstance(date, float):
        return timestamp_parser(date)
    else:
        return datetime_parser(date)


def convert_datetime64_to_netcdf_timestamp(
    datetime_value: numpy.datetime64,
    cftime_unit: str,
) -> int:
    pandas_datetime = pd.to_datetime(datetime_value)
    return cftime.date2num(pandas_datetime, cftime_unit)


def add_copernicusmarine_version_in_dataset_attributes(
    dataset: xarray.Dataset,
) -> xarray.Dataset:
    dataset.attrs["copernicusmarine_version"] = version("copernicusmarine")
    return dataset


# From: https://stackoverflow.com/a/46144596/20983727
def run_concurrently(
    func: Callable[..., _T],
    function_arguments: Sequence[tuple[Any, ...]],
    max_concurrent_requests: int,
    tdqm_bar_configuration: dict = {},
) -> list[_T]:
    out = []
    with tqdm(
        total=len(function_arguments),
        **tdqm_bar_configuration,
    ) as pbar:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=max_concurrent_requests
        ) as executor:
            future_to_url = (
                executor.submit(func, *function_argument)
                for function_argument in function_arguments
            )
            for future in concurrent.futures.as_completed(future_to_url):
                data = future.result()
                out.append(data)
                pbar.update(1)
    return out


# Example data_path
# https://s3.waw3-1.cloudferro.com/mdl-native-01/native/NWSHELF_MULTIYEAR_BGC_004_011/cmems_mod_nws_bgc-pft_myint_7km-3D-diato_P1M-m_202105
# https://s3.region.cloudferro.com/bucket/arco/product/dataset/geoChunked.zarr
# https://s3.region.cloudferro.com:443/bucket/arco/product/dataset/geoChunked.zarr
def parse_access_dataset_url(
    data_path: str, only_dataset_root_path: bool = False
) -> tuple[str, str, str]:

    match = re.search(
        r"^(http|https):\/\/([\w\-\.]+)(:[\d]+)?(\/.*)", data_path
    )
    if match:
        endpoint_url = match.group(1) + "://" + match.group(2)
        full_path = match.group(4)
        segments = full_path.split("/")
        bucket = segments[1]
        path = (
            "/".join(segments[2:])
            if not only_dataset_root_path
            else "/".join(segments[2:5]) + "/"
        )
        return endpoint_url, bucket, path
    else:
        raise Exception(f"Invalid data path: {data_path}")


def create_custom_query_function(username: Optional[str]) -> Callable:
    def _add_custom_query_param(params, context, **kwargs):
        """
        Add custom query params for MDS's Monitoring
        """
        params["url"] = construct_url_with_query_params(
            params["url"],
            construct_query_params_for_marine_data_store_monitoring(username),
        )

    return _add_custom_query_param


# Deprecation utils
def get_deprecated_message(old_value, preferred_value):
    return (
        f"'{old_value}' has been deprecated, use '{preferred_value}' instead"
    )


def log_deprecated_message(old_value, preferred_value):
    logger.warning(get_deprecated_message(old_value, preferred_value))


def raise_both_old_and_new_value_error(old_value, new_value):
    raise TypeError(
        f"Received both {old_value} and {new_value} as arguments! "
        f"{get_deprecated_message(old_value, new_value)}"
    )


def deprecated_python_option(**aliases: str) -> Callable:
    def deco(f: Callable):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            rename_kwargs(f.__name__, kwargs, aliases)
            return f(*args, **kwargs)

        return wrapper

    return deco


def rename_kwargs(
    func_name: str, kwargs: dict[str, Any], aliases: dict[str, str]
):
    for alias, new in aliases.items():
        if alias in kwargs:
            if new in kwargs:
                raise_both_old_and_new_value_error(alias, new)
            log_deprecated_message(alias, new)
            kwargs[new] = kwargs.pop(alias)
