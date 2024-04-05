import asyncio
import logging
import os
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from importlib.metadata import version as package_version
from itertools import groupby
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

import nest_asyncio
import pystac
from aiohttp import ContentTypeError
from cachier.core import cachier
from tqdm import tqdm

from copernicusmarine.aioretry import RetryInfo, RetryPolicyStrategy, retry
from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_debug,
)
from copernicusmarine.core_functions.sessions import (
    get_configured_aiohttp_session,
)
from copernicusmarine.core_functions.utils import (
    CACHE_BASE_DIRECTORY,
    construct_query_params_for_marine_data_store_monitoring,
    datetime_parser,
    map_reject_none,
    next_or_raise_exception,
    rolling_batch_gather,
)

logger = logging.getLogger("copernicus_marine_root_logger")

_S = TypeVar("_S")
_T = TypeVar("_T")


class _ServiceName(str, Enum):
    GEOSERIES = "arco-geo-series"
    TIMESERIES = "arco-time-series"
    FILES = "original-files"
    WMTS = "wmts"
    OMI_ARCO = "omi-arco"
    STATIC_ARCO = "static-arco"


class _ServiceShortName(str, Enum):
    GEOSERIES = "geoseries"
    TIMESERIES = "timeseries"
    FILES = "files"
    WMTS = "wmts"
    OMI_ARCO = "omi-arco"
    STATIC_ARCO = "static-arco"


MARINE_DATA_STORE_STAC_BASE_URL = "https://stac.marine.copernicus.eu/metadata"
MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL = (
    MARINE_DATA_STORE_STAC_BASE_URL + "/catalog.stac.json"
)
MARINE_DATA_STORE_STAC_BASE_URL_STAGING = (
    "https://stac-dta.marine.copernicus.eu/metadata"
)
MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL_STAGING = (
    MARINE_DATA_STORE_STAC_BASE_URL_STAGING + "/catalog.stac.json"
)

MAX_CONCURRENT_REQUESTS = int(
    os.getenv("COPERNICUSMARINE_MAX_CONCURRENT_REQUESTS", "15")
)


@dataclass(frozen=True)
class _Service:
    service_name: _ServiceName
    short_name: _ServiceShortName

    def aliases(self) -> List[str]:
        return (
            [self.service_name.value, self.short_name.value]
            if self.short_name.value != self.service_name.value
            else [self.service_name.value]
        )

    def to_json_dict(self):
        return {
            "service_name": self.service_name.value,
            "short_name": self.short_name.value,
        }


class CopernicusMarineDatasetServiceType(_Service, Enum):
    GEOSERIES = _ServiceName.GEOSERIES, _ServiceShortName.GEOSERIES
    TIMESERIES = (
        _ServiceName.TIMESERIES,
        _ServiceShortName.TIMESERIES,
    )
    FILES = _ServiceName.FILES, _ServiceShortName.FILES
    WMTS = _ServiceName.WMTS, _ServiceShortName.WMTS
    OMI_ARCO = _ServiceName.OMI_ARCO, _ServiceShortName.OMI_ARCO
    STATIC_ARCO = _ServiceName.STATIC_ARCO, _ServiceShortName.STATIC_ARCO


class CopernicusMarineServiceFormat(str, Enum):
    ZARR = "zarr"
    SQLITE = "sqlite"


def _service_type_from_web_api_string(
    name: str,
) -> CopernicusMarineDatasetServiceType:
    class WebApi(Enum):
        GEOSERIES = "timeChunked"
        TIMESERIES = "geoChunked"
        FILES = "native"
        WMTS = "wmts"
        OMI_ARCO = "omi"
        STATIC_ARCO = "static"

    web_api_mapping = {
        WebApi.GEOSERIES: CopernicusMarineDatasetServiceType.GEOSERIES,
        WebApi.TIMESERIES: CopernicusMarineDatasetServiceType.TIMESERIES,
        WebApi.FILES: CopernicusMarineDatasetServiceType.FILES,
        WebApi.WMTS: CopernicusMarineDatasetServiceType.WMTS,
        WebApi.OMI_ARCO: CopernicusMarineDatasetServiceType.OMI_ARCO,
        WebApi.STATIC_ARCO: CopernicusMarineDatasetServiceType.STATIC_ARCO,
    }

    return next_or_raise_exception(
        (
            service_type
            for service_web_api, service_type in web_api_mapping.items()
            if service_web_api.value == name
        ),
        ServiceNotHandled(name),
    )


class ServiceNotHandled(Exception):
    ...


VERSION_DEFAULT = "default"
PART_DEFAULT = "default"


@dataclass
class CopernicusMarineCoordinates:
    coordinates_id: str
    units: str
    minimum_value: Optional[float]
    maximum_value: Optional[float]
    step: Optional[float]
    values: Optional[str]
    chunking_length: Optional[int]
    chunk_type: Optional[str]
    chunk_reference_coordinate: Optional[int]
    chunk_geometric_factor: Optional[int]


@dataclass
class CopernicusMarineVariable:
    short_name: str
    standard_name: str
    units: str
    bbox: Tuple[float, float, float, float]
    coordinates: list[CopernicusMarineCoordinates]


@dataclass
class CopernicusMarineService:
    service_type: CopernicusMarineDatasetServiceType
    service_format: Optional[CopernicusMarineServiceFormat]
    uri: str
    variables: list[CopernicusMarineVariable]


@dataclass
class CopernicusMarineVersionPart:
    name: str
    services: list[CopernicusMarineService]
    retired_date: Optional[str]

    def get_service_by_service_type(
        self, service_type: CopernicusMarineDatasetServiceType
    ):
        return next(
            service
            for service in self.services
            if service.service_type == service_type
        )


@dataclass
class CopernicusMarineDatasetVersion:
    label: str
    parts: list[CopernicusMarineVersionPart]

    def get_part(
        self, force_part: Optional[str] = None
    ) -> CopernicusMarineVersionPart:
        wanted_part = force_part or PART_DEFAULT
        for part in self.parts:
            if part.name == wanted_part:
                return part
            elif not force_part:
                # TODO: If a dataset version has a non default part,
                # then return the first one for the moment
                return part
        raise dataset_version_part_not_found_exception(self)


class DatasetVersionPartNotFound(Exception):
    ...


class DatasetVersionNotFound(Exception):
    ...


@dataclass
class CopernicusMarineProductDataset:
    dataset_id: str
    dataset_name: str
    versions: list[CopernicusMarineDatasetVersion]

    def _seperate_version_and_default(
        self,
    ) -> Tuple[
        Optional[CopernicusMarineDatasetVersion],
        List[CopernicusMarineDatasetVersion],
    ]:
        default_version = None
        versions = []
        for version in self.versions:
            if version.label == VERSION_DEFAULT:
                default_version = version
            else:
                versions.append(version)
        return default_version, versions

    def get_latest_version_or_raise(self) -> CopernicusMarineDatasetVersion:
        default_version, versions = self._seperate_version_and_default()
        sorted_versions = sorted(versions, key=lambda x: x.label)
        if sorted_versions:
            return sorted_versions[-1]
        if default_version:
            return default_version
        raise dataset_version_not_found_exception(self)

    def get_latest_version(self) -> Optional[CopernicusMarineDatasetVersion]:
        try:
            return self.get_latest_version_or_raise()
        except DatasetVersionNotFound:
            return None


def dataset_version_part_not_found_exception(
    version: CopernicusMarineDatasetVersion,
) -> DatasetVersionPartNotFound:
    return DatasetVersionPartNotFound(
        f"No part found for version {version.label}"
    )


def dataset_version_not_found_exception(
    dataset: CopernicusMarineProductDataset,
) -> DatasetVersionNotFound:
    return DatasetVersionNotFound(
        f"No version found for dataset {dataset.dataset_id}"
    )


@dataclass
class CopernicusMarineProductProvider:
    name: str
    roles: list[str]
    url: str
    email: str


@dataclass
class CopernicusMarineProduct:
    title: str
    product_id: str
    thumbnail_url: str
    description: str
    digital_object_identifier: Optional[str]
    sources: List[str]
    processing_level: Optional[str]
    production_center: str
    keywords: dict[str, str]
    datasets: list[CopernicusMarineProductDataset]


@dataclass
class ProductDatasetParser(ABC):
    dataset_id: str
    dataset_name: str
    versions: list[CopernicusMarineDatasetVersion]

    @abstractmethod
    def to_copernicus_marine_dataset(
        self,
    ) -> CopernicusMarineProductDataset:
        ...


@dataclass
class ProductParser(ABC):
    title: str
    product_id: str
    thumbnail_url: str
    description: str
    digital_object_identifier: Optional[str]
    sources: List[str]
    processing_level: Optional[str]
    production_center: str
    keywords: dict[str, str]


@dataclass
class ProductDatasetFromMarineDataStore(ProductDatasetParser):
    def to_copernicus_marine_dataset(self) -> CopernicusMarineProductDataset:
        return CopernicusMarineProductDataset(
            dataset_id=self.dataset_id,
            dataset_name=self.dataset_name,
            versions=self.versions,
        )


@dataclass
class ProductFromMarineDataStore(ProductParser):
    datasets: list[ProductDatasetFromMarineDataStore]

    def to_copernicus_marine_product(self) -> CopernicusMarineProduct:
        return CopernicusMarineProduct(
            title=self.title,
            product_id=self.product_id,
            thumbnail_url=self.thumbnail_url,
            description=self.description,
            digital_object_identifier=self.digital_object_identifier,
            sources=self.sources,
            processing_level=self.processing_level,
            production_center=self.production_center,
            keywords=self.keywords,
            datasets=[
                dataset.to_copernicus_marine_dataset()
                for dataset in self.datasets
            ],
        )


@dataclass
class CopernicusMarineCatalogue:
    products: list[CopernicusMarineProduct]

    def filter(self, tokens: list[str]):
        return filter_catalogue_with_strings(self, tokens)


class CatalogParserConnection:
    def __init__(self, proxy: Optional[str] = None) -> None:
        self.proxy = proxy
        self.session = get_configured_aiohttp_session()
        self.__max_retries = 5
        self.__sleep_time = 1

    @retry("_retry_policy")
    async def get_json_file(self, url: str) -> dict[str, Any]:
        logger.debug(f"Fetching json file at this url: {url}")
        async with self.session.get(
            url,
            params=construct_query_params_for_marine_data_store_monitoring(),
        ) as response:
            return await response.json()

    async def close(self) -> None:
        await self.session.close()

    def _retry_policy(self, info: RetryInfo) -> RetryPolicyStrategy:
        if not isinstance(
            info.exception,
            (
                TimeoutError,
                ConnectionResetError,
                ContentTypeError,
            ),
        ):
            logger.error(
                f"Unexpected error while downloading: {info.exception}"
            )
            return True, 0
        logger.debug(
            f"Retrying {info.fails} times after error: {info.exception}"
        )
        return info.fails >= self.__max_retries, info.fails * self.__sleep_time


def _construct_copernicus_marine_service(
    stac_service_name, stac_asset, datacube
) -> Optional[CopernicusMarineService]:
    try:
        service_uri = stac_asset.get_absolute_href()
        service_type = _service_type_from_web_api_string(stac_service_name)
        service_format = None
        if stac_asset.media_type and "zarr" in stac_asset.media_type:
            service_format = CopernicusMarineServiceFormat.ZARR
        elif stac_asset.media_type and "sqlite3" in stac_asset.media_type:
            service_format = CopernicusMarineServiceFormat.SQLITE

        if not service_uri.endswith("/"):
            return CopernicusMarineService(
                service_type=service_type,
                uri=service_uri,
                variables=_get_variables(datacube, stac_asset),
                service_format=service_format,
            )
        return None
    except ServiceNotHandled as service_not_handled:
        log_exception_debug(service_not_handled)
        return None


def _get_versions_from_marine_datastore(
    datacubes: List[pystac.Item],
) -> List[CopernicusMarineDatasetVersion]:
    copernicus_marine_dataset_versions: List[
        CopernicusMarineDatasetVersion
    ] = []

    datacubes_by_version = groupby(
        datacubes,
        key=lambda datacube: get_version_and_part_from_full_dataset_id(
            datacube.id
        )[1],
    )
    for dataset_version, datacubes in datacubes_by_version:  # type: ignore
        parts = _get_parts(datacubes)

        if parts:
            copernicus_marine_dataset_versions.append(
                CopernicusMarineDatasetVersion(
                    label=dataset_version, parts=parts
                )
            )
    return copernicus_marine_dataset_versions


def _get_parts(
    datacubes: List[pystac.Item],
) -> List[CopernicusMarineVersionPart]:
    parts: List[CopernicusMarineVersionPart] = []

    for datacube in datacubes:
        retired_date = datacube.properties.get("admp_retired_date")
        if retired_date and datetime_parser(retired_date) < datetime_parser(
            "now"
        ):
            continue

        services = _get_services(datacube)
        _, _, part = get_version_and_part_from_full_dataset_id(datacube.id)

        if services:
            parts.append(
                CopernicusMarineVersionPart(
                    name=part,
                    services=_get_services(datacube),
                    retired_date=retired_date,
                )
            )

    if parts:
        return parts
    return []


def _get_services(
    datacube: pystac.Item,
) -> list[CopernicusMarineService]:
    stac_assets_dict = datacube.get_assets()
    return [
        dataset_service
        for stac_service_name, stac_asset in stac_assets_dict.items()
        if stac_asset.roles and "data" in stac_asset.roles
        if (
            dataset_service := _construct_copernicus_marine_service(
                stac_service_name, stac_asset, datacube
            )
        )
        is not None
    ]


def _format_arco_data_metadata_producer_valid_start_date(
    arco_data_metadata_producer_valid_start_date: str,
    to_timestamp: bool = False,
) -> Union[str, int]:
    if to_timestamp:
        return int(
            datetime_parser(
                arco_data_metadata_producer_valid_start_date.split(".")[0]
            ).timestamp()
            * 1000
        )
    return arco_data_metadata_producer_valid_start_date.split(".")[0]


def _get_variables(
    stac_dataset: pystac.Item,
    stac_asset: pystac.Asset,
) -> list[CopernicusMarineVariable]:
    bbox = stac_dataset.bbox
    return [
        CopernicusMarineVariable(
            short_name=var_cube["id"],
            standard_name=var_cube["standardName"],
            units=var_cube.get("unit") or "",
            bbox=bbox,
            coordinates=_get_coordinates(
                var_cube["id"],
                stac_asset,
                stac_dataset.properties.get("admp_valid_start_date"),
            )
            or [],
        )
        for var_cube in stac_dataset.properties["cube:variables"].values()
    ]


def _get_coordinates(
    variable_id: str,
    stac_asset: pystac.Asset,
    arco_data_metadata_producer_valid_start_date: Optional[str],
) -> Optional[list[CopernicusMarineCoordinates]]:
    extra_fields_asset = stac_asset.extra_fields
    dimensions = extra_fields_asset.get("viewDims")
    if dimensions:
        coordinates = []
        for dimension, dimension_metadata in dimensions.items():
            coordinates_info = dimension_metadata.get("coords", {})
            if (
                arco_data_metadata_producer_valid_start_date
                and dimension == "time"
            ):
                minimum_value = (
                    _format_arco_data_metadata_producer_valid_start_date(
                        arco_data_metadata_producer_valid_start_date,
                        to_timestamp=isinstance(
                            coordinates_info.get("min"), int
                        ),
                    )
                )
            else:
                minimum_value = coordinates_info.get("min")
            chunking_length = dimension_metadata.get("chunkLen")
            if isinstance(chunking_length, dict):
                chunking_length = chunking_length.get(variable_id)
            coordinates.append(
                CopernicusMarineCoordinates(
                    coordinates_id=(
                        "depth" if dimension == "elevation" else dimension
                    ),
                    units=dimension_metadata.get("units") or "",
                    minimum_value=minimum_value,  # type: ignore
                    maximum_value=coordinates_info.get("max"),
                    step=coordinates_info.get("step"),
                    values=coordinates_info.get("values"),
                    chunking_length=chunking_length,
                    chunk_type=dimension_metadata.get("chunkType"),
                    chunk_reference_coordinate=dimension_metadata.get(
                        "chunkRefCoord"
                    ),
                    chunk_geometric_factor=dimension_metadata.get(
                        "chunkGeometricFactor", {}
                    ).get(variable_id),
                )
            )
        return coordinates
    else:
        return None


def _construct_marine_data_store_dataset(
    datacubes_by_id: List,
) -> Optional[ProductDatasetFromMarineDataStore]:
    dataset_id = datacubes_by_id[0]
    datacubes = list(datacubes_by_id[1])
    dataset_name = (
        datacubes[0].properties["title"] if len(datacubes) == 1 else dataset_id
    )
    if datacubes:
        versions = _get_versions_from_marine_datastore(datacubes)
        if versions:
            return ProductDatasetFromMarineDataStore(
                dataset_id=dataset_id,
                dataset_name=dataset_name,
                versions=versions,
            )
    return None


def _construct_marine_data_store_product(
    stac_tuple: Tuple[pystac.Collection, List[pystac.Item]],
) -> ProductFromMarineDataStore:
    stac_product, stac_datasets = stac_tuple
    stac_datasets_sorted = sorted(stac_datasets, key=lambda x: x.id)
    datacubes_by_id = groupby(
        stac_datasets_sorted,
        key=lambda x: get_version_and_part_from_full_dataset_id(x.id)[0],
    )

    datasets = map(
        _construct_marine_data_store_dataset,  # type: ignore
        datacubes_by_id,  # type: ignore
    )

    production_center = [
        provider.name
        for provider in stac_product.providers or []
        if "producer" in provider.roles
    ]

    production_center_name = production_center[0] if production_center else ""

    thumbnail = stac_product.assets and stac_product.assets.get("thumbnail")
    digital_object_identifier = (
        stac_product.extra_fields.get("sci:doi", None)
        if stac_product.extra_fields
        else None
    )
    sources = _get_stac_product_property(stac_product, "sources") or []
    processing_level = _get_stac_product_property(
        stac_product, "processingLevel"
    )

    return ProductFromMarineDataStore(
        title=stac_product.title or stac_product.id,
        product_id=stac_product.id,
        thumbnail_url=thumbnail.get_absolute_href() if thumbnail else "",
        description=stac_product.description,
        digital_object_identifier=digital_object_identifier,
        sources=sources,
        processing_level=processing_level,
        production_center=production_center_name,
        keywords=stac_product.keywords,
        datasets=sorted(
            [dataset for dataset in datasets if dataset],
            key=lambda dataset: dataset.dataset_id,
        ),
    )


def _get_stac_product_property(
    stac_product: pystac.Collection, property_key: str
) -> Optional[Any]:
    properties: Dict[str, str] = (
        stac_product.extra_fields.get("properties", {})
        if stac_product.extra_fields
        else {}
    )
    return properties.get(property_key)


async def async_fetch_items_from_collection(
    root_url: str,
    connection: CatalogParserConnection,
    collection: pystac.Collection,
) -> List[pystac.Item]:
    items = []
    for link in collection.get_item_links():
        if not link.owner:
            logger.warning(f"Invalid Item, no owner for: {link.href}")
            continue
        url = root_url + "/" + link.owner.id + "/" + link.href
        try:
            item_json = await connection.get_json_file(url)
            items.append(pystac.Item.from_dict(item_json))
        except pystac.STACError as exception:
            message = (
                "Invalid Item: If datetime is None, a start_datetime "
                + "and end_datetime must be supplied."
            )
            if exception.args[0] != message:
                logger.error(exception)
                raise pystac.STACError(exception.args)
    return items


async def async_fetch_collection(
    root_url: str, connection: CatalogParserConnection, url: str
) -> Optional[Tuple[pystac.Collection, List[pystac.Item]]]:
    json_collection = await connection.get_json_file(url)
    try:
        collection = pystac.Collection.from_dict(json_collection)
        items = await async_fetch_items_from_collection(
            root_url, connection, collection
        )
        return (collection, items)

    except KeyError as exception:
        messages = ["spatial", "temporal"]
        if exception.args[0] not in messages:
            logger.error(exception)
        return None


async def async_fetch_childs(
    root_url: str,
    connection: CatalogParserConnection,
    child_links: List[pystac.Link],
) -> Iterator[Optional[Tuple[pystac.Collection, List[pystac.Item]]]]:
    tasks = []
    for link in child_links:
        tasks.append(
            async_fetch_collection(root_url, connection, link.absolute_href)
        )
    return filter(
        lambda x: x is not None,
        await rolling_batch_gather(tasks, MAX_CONCURRENT_REQUESTS),
    )


async def async_fetch_catalog(
    connection: CatalogParserConnection,
    staging: bool = False,
) -> Iterator[pystac.Collection]:
    catalog_root_url = (
        MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL
        if not staging
        else MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL_STAGING
    )
    json_catalog = await connection.get_json_file(catalog_root_url)
    catalog = pystac.Catalog.from_dict(json_catalog)
    child_links = catalog.get_child_links()
    root_url = (
        MARINE_DATA_STORE_STAC_BASE_URL
        if not staging
        else (MARINE_DATA_STORE_STAC_BASE_URL_STAGING)
    )
    childs = await async_fetch_childs(root_url, connection, child_links)
    return childs


def _retrieve_marine_data_store_products(
    connection: CatalogParserConnection,
    staging: bool = False,
) -> list[ProductFromMarineDataStore]:
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    marine_data_store_root_collections = loop.run_until_complete(
        async_fetch_catalog(connection=connection, staging=staging)
    )

    products = map_reject_none(
        _construct_marine_data_store_product,
        marine_data_store_root_collections,
    )

    return list(products)


def parse_catalogue(
    no_metadata_cache: bool,
    disable_progress_bar: bool,
    staging: bool = False,
) -> CopernicusMarineCatalogue:
    logger.debug("Parsing catalogue...")
    try:
        catalog = _parse_catalogue(
            ignore_cache=no_metadata_cache,
            _versions=package_version("copernicusmarine"),
            disable_progress_bar=disable_progress_bar,
            staging=staging,
        )
    except ValueError:
        catalog = _parse_catalogue(
            ignore_cache=True,
            _versions=package_version("copernicusmarine"),
            disable_progress_bar=disable_progress_bar,
            staging=staging,
        )
    logger.debug("Catalogue parsed")
    return catalog


@cachier(cache_dir=CACHE_BASE_DIRECTORY, stale_after=timedelta(hours=24))
def _parse_catalogue(
    _versions: str,  # force cachier to overwrite cache in case of version update
    disable_progress_bar: bool,
    staging: bool = False,
) -> CopernicusMarineCatalogue:
    progress_bar = tqdm(
        total=3, desc="Fetching catalog", disable=disable_progress_bar
    )
    connection = CatalogParserConnection()

    marine_data_store_products = _retrieve_marine_data_store_products(
        connection=connection, staging=staging
    )
    progress_bar.update()

    products_merged: List[CopernicusMarineProduct] = [
        marine_data_store_product.to_copernicus_marine_product()
        for marine_data_store_product in marine_data_store_products
        if marine_data_store_product.datasets
    ]
    products_merged.sort(key=lambda x: x.product_id)
    progress_bar.update()

    full_catalog = CopernicusMarineCatalogue(products=products_merged)

    progress_bar.update()
    asyncio.run(connection.close())

    return full_catalog


def variable_title_to_standard_name(variable_title: str) -> str:
    return variable_title.lower().replace(" ", "_")


def variable_to_pick(layer: dict[str, Any]) -> bool:
    return (
        layer["variableId"] != "__DEFAULT__"
        and layer["subsetVariableIds"]
        and len(layer["subsetVariableIds"]) == 1
    )


def _to_service(
    service_name: str,
    stac_asset: dict,
    layer_elements,
) -> Optional[CopernicusMarineService]:
    service_format_asset = stac_asset.get("type")
    service_url = stac_asset.get("href")
    try:
        service_type = _service_type_from_web_api_string(service_name)
        service_format = None
        if service_format_asset and "zarr" in service_format_asset:
            service_format = CopernicusMarineServiceFormat.ZARR
        elif service_format_asset and "sqlite3" in service_format_asset:
            service_format = CopernicusMarineServiceFormat.SQLITE
        if service_url and not service_url.endswith("thredds/dodsC/"):
            return CopernicusMarineService(
                service_type=service_type,
                uri=service_url,
                service_format=service_format,
                variables=[
                    to_variable(layer, stac_asset)
                    for layer in layer_elements
                    if variable_to_pick(layer)
                ],
            )
        else:
            return None
    except ServiceNotHandled as service_not_handled:
        log_exception_debug(service_not_handled)
        return None


def to_coordinates(
    subset_attributes: Tuple[str, dict[str, Any]],
    layer: dict[str, Any],
    asset: dict,
) -> CopernicusMarineCoordinates:
    coordinate_name = subset_attributes[0]
    values: Optional[str]
    if coordinate_name == "depth":
        values = layer.get("zValues")
    elif coordinate_name == "time":
        values = layer.get("tValues")
    else:
        values = None
    view_dim = asset.get("viewDims", {}).get(coordinate_name, {})
    chunking_length = view_dim.get("chunkLen")
    if isinstance(chunking_length, dict):
        chunking_length = chunking_length.get(layer["variableId"])
    return CopernicusMarineCoordinates(
        coordinates_id=subset_attributes[0],
        units=view_dim.get("units", ""),
        minimum_value=view_dim.get("min") or view_dim.get(values, [None])[0],
        maximum_value=view_dim.get("max") or view_dim.get(values, [None])[0],
        step=view_dim.get("step"),
        values=values,
        chunking_length=chunking_length,
        chunk_type=view_dim.get("chunkType"),
        chunk_reference_coordinate=view_dim.get("chunkRefCoord"),
        chunk_geometric_factor=view_dim.get("chunkGeometricFactor", {})
        .get("chunkGeometricFactor", {})
        .get(layer["variableId"]),
    )


def to_variable(
    layer: dict[str, Any], asset: dict
) -> CopernicusMarineVariable:
    return CopernicusMarineVariable(
        short_name=layer["variableId"],
        standard_name=variable_title_to_standard_name(layer["variableTitle"]),
        units=layer["units"],
        bbox=layer["bbox"],
        coordinates=[
            to_coordinates(subset_attr, layer, asset)
            for subset_attr in layer["subsetAttrs"].items()
        ],
    )


@dataclass
class DistinctDatasetVersionPart:
    dataset_id: str
    dataset_version: str
    dataset_part: str
    layer_elements: List
    raw_services: Dict
    stac_items_values: Optional[Dict]


def mds_stac_to_services(
    distinct_dataset_version: DistinctDatasetVersionPart,
) -> List[CopernicusMarineService]:
    copernicus_marine_services = []

    stac_assets = (
        distinct_dataset_version.stac_items_values
        and distinct_dataset_version.stac_items_values.get("assets", None)
    )
    if stac_assets:
        for (
            service_name,
            service_url,
        ) in stac_assets.items():
            service = _to_service(
                service_name,
                service_url,
                distinct_dataset_version.layer_elements,
            )
            if service:
                copernicus_marine_services.append(service)

    return copernicus_marine_services


REGEX_PATTERN_DATE_YYYYMM = r"[12]\d{3}(0[1-9]|1[0-2])"
PART_SEPARATOR = "--ext--"


def get_version_and_part_from_full_dataset_id(
    full_dataset_id: str,
) -> Tuple[str, str, str]:
    if PART_SEPARATOR in full_dataset_id:
        name_with_maybe_version, part = full_dataset_id.split(PART_SEPARATOR)
    else:
        name_with_maybe_version = full_dataset_id
        part = PART_DEFAULT
    pattern = rf"^(.*?)(?:_({REGEX_PATTERN_DATE_YYYYMM}))?$"
    match = re.match(pattern, name_with_maybe_version)
    if match:
        dataset_name = match.group(1)
        version = match.group(2) or VERSION_DEFAULT
    else:
        raise Exception(f"Could not parse dataset id: {full_dataset_id}")
    return dataset_name, version, part


# ---------------------------------------
# --- Utils function on any catalogue ---
# ---------------------------------------


def get_product_from_url(
    catalogue: CopernicusMarineCatalogue, dataset_url: str
) -> CopernicusMarineProduct:
    """
    Return the product object, with its dataset list filtered
    """
    filtered_catalogue = filter_catalogue_with_strings(
        catalogue, [dataset_url]
    )
    if filtered_catalogue is None:
        error = TypeError("filtered catalogue is empty")
        raise error
    if isinstance(filtered_catalogue, CopernicusMarineCatalogue):
        return filtered_catalogue.products[0]
    return filtered_catalogue["products"][0]


def filter_catalogue_with_strings(
    catalogue: CopernicusMarineCatalogue, tokens: list[str]
) -> dict[str, Any]:
    return find_match_object(catalogue, tokens) or {}


def find_match_object(value: Any, tokens: list[str]) -> Any:
    match: Any
    if isinstance(value, str):
        match = find_match_string(value, tokens)
    elif isinstance(value, Enum):
        match = find_match_enum(value, tokens)
    elif isinstance(value, tuple):
        match = find_match_tuple(value, tokens)
    elif isinstance(value, list):
        match = find_match_list(value, tokens)
    elif hasattr(value, "__dict__"):
        match = find_match_dict(value, tokens)
    else:
        match = None
    return match


def find_match_string(string: str, tokens: list[str]) -> Optional[str]:
    return string if any(token in string for token in tokens) else None


def find_match_enum(enum: Enum, tokens: list[str]) -> Any:
    return find_match_object(enum.value, tokens)


def find_match_tuple(tuple: Tuple, tokens: list[str]) -> Optional[list[Any]]:
    return find_match_list(list(tuple), tokens)


def find_match_list(object_list: list[Any], tokens) -> Optional[list[Any]]:
    def find_match(element: Any) -> Optional[Any]:
        return find_match_object(element, tokens)

    filtered_list: list[Any] = list(map_reject_none(find_match, object_list))
    return filtered_list if filtered_list else None


def find_match_dict(
    structure: dict[str, Any], tokens
) -> Optional[dict[str, Any]]:
    filtered_dict = {
        key: find_match_object(value, tokens)
        for key, value in structure.__dict__.items()
        if find_match_object(value, tokens)
    }

    found_match = any(filtered_dict.values())
    if found_match:
        new_dict = dict(structure.__dict__, **filtered_dict)
        structure.__dict__ = new_dict
    return structure if found_match else None
