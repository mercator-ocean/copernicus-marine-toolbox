import concurrent.futures
import logging
import pathlib
import re
from datetime import datetime, timezone
from typing import (
    Any,
    Callable,
    Iterator,
    Literal,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import numpy
import xarray
from dateutil import parser
from dateutil.parser._parser import ParserError
from requests import PreparedRequest
from tqdm import tqdm

from copernicusmarine.core_functions.exceptions import WrongDatetimeFormat
from copernicusmarine.versioner import __version__ as copernicusmarine_version

logger = logging.getLogger("copernicusmarine")


def get_unique_filename(
    filepath: pathlib.Path,
) -> pathlib.Path:
    parent = filepath.parent
    filename = filepath.stem
    extension = filepath.suffix
    counter = 1

    while filepath.exists():
        filepath = parent / (filename + "_(" + str(counter) + ")" + extension)
        counter += 1
    return filepath


_T = TypeVar("_T")


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


def datetime_parser(date: Union[str, numpy.datetime64]) -> datetime:
    if date == "now":
        return datetime.now(tz=timezone.utc)
    try:
        if isinstance(date, numpy.datetime64):
            date = str(date)
        parsed_datetime = parser.parse(
            date, default=datetime(1978, 1, 1, 0, 0, 0, 0, tzinfo=timezone.utc)
        )
        # # add timezone info if not present
        # if parsed_datetime.tzinfo is None:
        #     parsed_datetime = parsed_datetime.replace(tzinfo=timezone.utc)
        return parsed_datetime
    except ParserError:
        pass
    raise WrongDatetimeFormat(date)


def timestamp_parser(
    timestamp: Union[int, float], unit: Literal["s", "ms"] = "ms"
) -> datetime:
    """
    Convert a timestamp in milliseconds to a datetime object.
    The unit can be changed to seconds by passing "s".
    """
    conversion_factor = 1 if unit == "s" else 1e3

    return datetime.fromtimestamp(
        timestamp / conversion_factor, tz=timezone.utc
    )


def timestamp_or_datestring_to_datetime(
    date: Union[str, int, float, numpy.datetime64]
) -> datetime:
    if isinstance(date, int) or isinstance(date, float):
        return timestamp_parser(date)
    else:
        return datetime_parser(date)


def add_copernicusmarine_version_in_dataset_attributes(
    dataset: xarray.Dataset,
) -> xarray.Dataset:
    dataset.attrs["copernicusmarine_version"] = copernicusmarine_version
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
