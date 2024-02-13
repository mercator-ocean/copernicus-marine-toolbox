import asyncio
import logging
import re
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from importlib.metadata import version as package_version
from itertools import chain, groupby, repeat
from json import loads
from typing import Any, Dict, Iterator, List, Optional, Tuple, TypeVar, Union

import nest_asyncio
import pystac
from cachier import cachier
from tqdm import tqdm

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_debug,
)
from copernicusmarine.core_functions.sessions import (
    get_configured_aiohttp_session,
    get_configured_request_session,
)
from copernicusmarine.core_functions.utils import (
    CACHE_BASE_DIRECTORY,
    construct_query_params_for_marine_data_store_monitoring,
    map_reject_none,
    next_or_raise_exception,
)

logger = logging.getLogger("copernicus_marine_root_logger")

_S = TypeVar("_S")
_T = TypeVar("_T")


class _ServiceName(str, Enum):
    MOTU = "motu"
    OPENDAP = "opendap"
    GEOSERIES = "arco-geo-series"
    TIMESERIES = "arco-time-series"
    FILES = "original-files"
    FTP = "ftp"
    WMS = "wms"
    WMTS = "wmts"
    OMI_ARCO = "omi-arco"
    STATIC_ARCO = "static-arco"


class _ServiceShortName(str, Enum):
    MOTU = "motu"
    OPENDAP = "opendap"
    GEOSERIES = "geoseries"
    TIMESERIES = "timeseries"
    FILES = "files"
    FTP = "ftp"
    WMS = "wms"
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
    MOTU = _ServiceName.MOTU, _ServiceShortName.MOTU
    OPENDAP = _ServiceName.OPENDAP, _ServiceShortName.OPENDAP
    GEOSERIES = _ServiceName.GEOSERIES, _ServiceShortName.GEOSERIES
    TIMESERIES = (
        _ServiceName.TIMESERIES,
        _ServiceShortName.TIMESERIES,
    )
    FILES = _ServiceName.FILES, _ServiceShortName.FILES
    FTP = _ServiceName.FTP, _ServiceShortName.FTP
    WMS = _ServiceName.WMS, _ServiceShortName.WMS
    WMTS = _ServiceName.WMTS, _ServiceShortName.WMTS
    OMI_ARCO = _ServiceName.OMI_ARCO, _ServiceShortName.OMI_ARCO
    STATIC_ARCO = _ServiceName.STATIC_ARCO, _ServiceShortName.STATIC_ARCO


def _service_type_from_web_api_string(
    name: str,
) -> CopernicusMarineDatasetServiceType:
    class WebApi(Enum):
        MOTU = "motu"
        OPENDAP = "opendap"
        GEOSERIES = "timeChunked"
        TIMESERIES = "geoChunked"
        FILES = "native"
        FTP = "ftp"
        WMS = "wms"
        WMTS = "wmts"
        OMI_ARCO = "omi"
        STATIC_ARCO = "static"

    web_api_mapping = {
        WebApi.MOTU: CopernicusMarineDatasetServiceType.MOTU,
        WebApi.OPENDAP: CopernicusMarineDatasetServiceType.OPENDAP,
        WebApi.GEOSERIES: CopernicusMarineDatasetServiceType.GEOSERIES,
        WebApi.TIMESERIES: CopernicusMarineDatasetServiceType.TIMESERIES,
        WebApi.FILES: CopernicusMarineDatasetServiceType.FILES,
        WebApi.FTP: CopernicusMarineDatasetServiceType.FTP,
        WebApi.WMS: CopernicusMarineDatasetServiceType.WMS,
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
    uri: str
    variables: list[CopernicusMarineVariable]


@dataclass
class CopernicusMarineVersionPart:
    name: str
    services: list[CopernicusMarineService]

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

    def add_raw_services(
        self, raw_services: Dict, layer_elements: List
    ) -> None:
        latest_version = self.get_latest_version()

        portal_services = portal_services_to_services(
            raw_services,
            layer_elements,
        )
        if latest_version:
            for part in latest_version.parts:
                part.services += portal_services
        elif portal_services:
            self.versions.append(
                CopernicusMarineDatasetVersion(
                    label=VERSION_DEFAULT,
                    parts=[
                        CopernicusMarineVersionPart(
                            name=PART_DEFAULT, services=portal_services
                        )
                    ],
                )
            )


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
class ProductDatasetFromPortal(ProductDatasetParser):
    raw_services: Dict
    layer_elements: List

    def to_copernicus_marine_dataset(self) -> CopernicusMarineProductDataset:
        copernicus_marine_dataset = CopernicusMarineProductDataset(
            dataset_id=self.dataset_id,
            dataset_name=self.dataset_name,
            versions=self.versions,
        )
        copernicus_marine_dataset.add_raw_services(
            self.raw_services, self.layer_elements
        )
        return copernicus_marine_dataset


@dataclass
class ProductFromPortal(ProductParser):
    datasets: list[ProductDatasetFromPortal]

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

    async def get_json_file(self, url: str) -> dict[str, Any]:
        logger.debug(f"Fetching json file at this url: {url}")
        async with self.session.get(
            url,
            params=construct_query_params_for_marine_data_store_monitoring(),
        ) as response:
            return await response.json()

    async def close(self) -> None:
        await self.session.close()


def _construct_copernicus_marine_service(
    stac_service_name, stac_asset, datacube
) -> Optional[CopernicusMarineService]:
    try:
        service_uri = stac_asset.get_absolute_href()
        service_type = _service_type_from_web_api_string(stac_service_name)
        if service_type in (
            CopernicusMarineDatasetServiceType.GEOSERIES,
            CopernicusMarineDatasetServiceType.TIMESERIES,
        ):
            if not service_uri.endswith(".zarr"):
                return None
        if not service_uri.endswith("/"):
            return CopernicusMarineService(
                service_type=service_type,
                uri=stac_asset.get_absolute_href(),
                variables=_get_variables(datacube),
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
        services = _get_services(datacube)
        _, _, part = get_version_and_part_from_full_dataset_id(datacube.id)

        if services:
            parts.append(
                CopernicusMarineVersionPart(
                    name=part, services=_get_services(datacube)
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


def _get_coordinates(
    dimensions_cube: dict,
    arco_data_metadata_producer_valid_start_date: Optional[str],
) -> dict[str, CopernicusMarineCoordinates]:
    def _create_coordinate(
        key: str,
        value: dict,
        arco_data_metadata_producer_valid_start_date: Optional[str],
    ) -> CopernicusMarineCoordinates:
        if arco_data_metadata_producer_valid_start_date:
            minimum_value = (
                _format_arco_data_metadata_producer_valid_start_date(
                    arco_data_metadata_producer_valid_start_date
                )
            )
        else:
            minimum_value = value["extent"][0] if "extent" in value else None
        return CopernicusMarineCoordinates(
            coordinates_id="depth" if key == "elevation" else key,
            units=value.get("unit") or "",
            minimum_value=minimum_value,  # type: ignore
            maximum_value=value["extent"][1] if "extent" in value else None,
            step=value.get("step"),
            values=value.get("values"),
        )

    coordinates_dict = {}
    for key, value in dimensions_cube.items():
        coordinates_dict[key] = _create_coordinate(
            key,
            value,
            (
                arco_data_metadata_producer_valid_start_date
                if key == "time"
                else None
            ),
        )
    return coordinates_dict


def _format_arco_data_metadata_producer_valid_start_date(
    arco_data_metadata_producer_valid_start_date: str,
) -> str:
    return arco_data_metadata_producer_valid_start_date.split(".")[0]


def _get_variables(
    stac_dataset: pystac.Item,
) -> list[CopernicusMarineVariable]:
    def _create_variable(
        variable_cube: dict[str, Any],
        bbox: tuple[float, float, float, float],
        coordinates_dict: dict[str, CopernicusMarineCoordinates],
    ) -> Union[CopernicusMarineVariable, None]:
        coordinates = variable_cube["dimensions"]
        return CopernicusMarineVariable(
            short_name=variable_cube["id"],
            standard_name=variable_cube["standardName"],
            units=variable_cube.get("unit") or "",
            bbox=bbox,
            coordinates=[coordinates_dict[key] for key in coordinates],
        )

    coordinates_dict = _get_coordinates(
        stac_dataset.properties["cube:dimensions"],
        stac_dataset.properties.get("admp_valid_start_date"),
    )
    bbox = stac_dataset.bbox
    variables: list[Optional[CopernicusMarineVariable]] = []
    for var_cube in stac_dataset.properties["cube:variables"].values():
        variables += [_create_variable(var_cube, bbox, coordinates_dict)]
    return [var for var in variables if var]


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
        _construct_marine_data_store_dataset, datacubes_by_id  # type: ignore
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
            asyncio.ensure_future(
                async_fetch_collection(
                    root_url, connection, link.absolute_href
                )
            )
        )
    return filter(lambda x: x is not None, await asyncio.gather(*tasks))


async def async_fetch_catalog(
    connection: CatalogParserConnection,
    staging: bool = False,
) -> Tuple[pystac.Catalog, Iterator[pystac.Collection]]:
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
    return catalog, childs


def _retrieve_marine_data_store_products(
    connection: CatalogParserConnection,
    staging: bool = False,
) -> list[ProductFromMarineDataStore]:
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    _, marine_data_store_root_collections = loop.run_until_complete(
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
    catalog = _parse_catalogue(
        ignore_cache=no_metadata_cache,
        _version=package_version("copernicusmarine"),
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )
    logger.debug("Catalogue parsed")
    return catalog


def merge_products(
    products_from_marine_data_store: List[ProductFromMarineDataStore],
    products_from_portal: List[ProductFromPortal],
) -> List[CopernicusMarineProduct]:
    merged_products: List[CopernicusMarineProduct] = []
    for marine_data_store_product in products_from_marine_data_store:
        merged_products.append(
            marine_data_store_product.to_copernicus_marine_product()
        )

    for portal_product in products_from_portal:
        maybe_merged_product = list(
            filter(
                lambda x: x.product_id == portal_product.product_id,
                merged_products,
            )
        )
        if not maybe_merged_product:
            merged_product = portal_product.to_copernicus_marine_product()
            merged_products.append(merged_product)

        else:
            merged_product = maybe_merged_product[0]
            for portal_dataset in portal_product.datasets:
                maybe_merged_dataset = list(
                    filter(
                        lambda x: x.dataset_id == portal_dataset.dataset_id,
                        merged_product.datasets,
                    )
                )
                if not maybe_merged_dataset:
                    merged_product.datasets.append(
                        portal_dataset.to_copernicus_marine_dataset()
                    )
                else:
                    merged_dataset = maybe_merged_dataset[0]
                    for portal_version in portal_dataset.versions:
                        maybe_merged_version = list(
                            filter(
                                lambda x: x.label == portal_version.label,
                                merged_dataset.versions,
                            )
                        )
                        if not maybe_merged_version:
                            merged_dataset.versions.append(portal_version)
                        else:
                            merged_version = maybe_merged_version[0]
                            for portal_part in portal_version.parts:
                                maybe_merged_part = list(
                                    filter(
                                        lambda x: x.name == portal_part.name,
                                        merged_version.parts,
                                    )
                                )
                                if not maybe_merged_part:
                                    merged_version.parts.append(portal_part)
                                else:
                                    merged_part = maybe_merged_part[0]
                                    for portal_service in portal_part.services:
                                        maybe_merged_service = list(
                                            filter(
                                                lambda x: x.service_type
                                                == portal_service.service_type,
                                                merged_part.services,
                                            )
                                        )
                                        if not maybe_merged_service:
                                            merged_part.services.append(
                                                portal_service
                                            )
                                        else:
                                            continue
                                    merged_part.services.sort(
                                        key=lambda x: x.service_type.service_name.value
                                    )
                            merged_version.parts.sort(key=lambda x: x.name)

                    merged_dataset.versions.sort(
                        key=lambda x: (
                            x.label if x.label != VERSION_DEFAULT else "110001"
                        ),
                        reverse=True,
                    )

                    merged_dataset.add_raw_services(
                        portal_dataset.raw_services,
                        portal_dataset.layer_elements,
                    )
            merged_product.datasets.sort(key=lambda x: x.dataset_id)

    return merged_products


@cachier(cache_dir=CACHE_BASE_DIRECTORY, stale_after=timedelta(hours=24))
def _parse_catalogue(
    _version: str,  # force cachier to overwrite cache in case of version update
    disable_progress_bar: bool,
    staging: bool = False,
) -> CopernicusMarineCatalogue:
    progress_bar = tqdm(
        total=4, desc="Fetching catalog", disable=disable_progress_bar
    )
    connection = CatalogParserConnection()
    if not staging:
        logger.debug("Parsing portal catalogue...")
        try:
            portal_products = _parse_portal_backend_products(connection)
            progress_bar.update()
            logger.debug("Portal catalogue parsed")
        except Exception as e:
            logger.warning(
                f"Failed to parse portal catalogue: {e}. Only using stac catalogue."
            )
            progress_bar.update()
            portal_products = []
    else:
        # because of data misalignment we don't want to use data-be endpoint
        portal_products = []

    marine_data_store_products = _retrieve_marine_data_store_products(
        connection=connection, staging=staging
    )
    progress_bar.update()

    products_merged = merge_products(
        marine_data_store_products, portal_products
    )
    products_merged.sort(key=lambda x: x.product_id)
    progress_bar.update()

    full_catalog = CopernicusMarineCatalogue(products=products_merged)

    progress_bar.update()
    asyncio.run(connection.close())

    return full_catalog


async def _async_fetch_raw_products(
    product_ids: List[str], connection: CatalogParserConnection
):
    tasks = []
    for product_id in product_ids:
        tasks.append(
            asyncio.ensure_future(
                connection.get_json_file(product_url(product_id))
            )
        )

    return await asyncio.gather(*tasks)


def product_url(product_id: str) -> str:
    base_url = "https://data-be-prd.marine.copernicus.eu/api/dataset"
    return f"{base_url}/{product_id}" + "?variant=detailed-v2"


def variable_title_to_standard_name(variable_title: str) -> str:
    return variable_title.lower().replace(" ", "_")


def variable_to_pick(layer: dict[str, Any]) -> bool:
    return (
        layer["variableId"] != "__DEFAULT__"
        and layer["subsetVariableIds"]
        and len(layer["subsetVariableIds"]) == 1
    )


def _to_service(
    service_name: str, service_url: str, layer_elements
) -> Optional[CopernicusMarineService]:
    try:
        service_type = _service_type_from_web_api_string(service_name)
        if service_type in (
            CopernicusMarineDatasetServiceType.GEOSERIES,
            CopernicusMarineDatasetServiceType.TIMESERIES,
        ):
            if not service_url.endswith(".zarr"):
                return None
        if not service_url.endswith("thredds/dodsC/"):
            return CopernicusMarineService(
                service_type=service_type,
                uri=service_url,
                variables=list(
                    map(to_variable, filter(variable_to_pick, layer_elements))
                ),
            )
        else:
            return None
    except ServiceNotHandled as service_not_handled:
        log_exception_debug(service_not_handled)
        return None


def to_coordinates(
    subset_attributes: Tuple[str, dict[str, Any]], layer: dict[str, Any]
) -> CopernicusMarineCoordinates:
    coordinate_name = subset_attributes[0]
    values: Optional[str]
    if coordinate_name == "depth":
        values = layer.get("zValues")
    elif coordinate_name == "time":
        values = layer.get("tValues")
    else:
        values = None
    return CopernicusMarineCoordinates(
        coordinates_id=subset_attributes[0],
        units=subset_attributes[1]["units"],
        minimum_value=subset_attributes[1]["min"],
        maximum_value=subset_attributes[1]["max"],
        step=subset_attributes[1].get("step"),
        values=values,
    )


def to_variable(layer: dict[str, Any]) -> CopernicusMarineVariable:
    return CopernicusMarineVariable(
        short_name=layer["variableId"],
        standard_name=variable_title_to_standard_name(layer["variableTitle"]),
        units=layer["units"],
        bbox=layer["bbox"],
        coordinates=list(
            map(to_coordinates, layer["subsetAttrs"].items(), repeat(layer))
        ),
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
                service_url["href"],
                distinct_dataset_version.layer_elements,
            )
            if service:
                copernicus_marine_services.append(service)

    return copernicus_marine_services


def portal_services_to_services(
    raw_services: Dict, layer_elements: List
) -> list[CopernicusMarineService]:
    copernicus_marine_services = []

    for (
        service_name,
        service_url,
    ) in raw_services.items():
        copernicus_marine_services.append(
            _to_service(
                service_name,
                service_url,
                layer_elements,
            )
        )

    return [service for service in copernicus_marine_services if service]


def to_dataset(
    distinct_dataset_versions: List[DistinctDatasetVersionPart],
) -> Optional[ProductDatasetFromPortal]:
    if distinct_dataset_versions:
        first_distinct_dataset_version = distinct_dataset_versions[0]
        dataset_id = first_distinct_dataset_version.dataset_id
        layer_elements = list(first_distinct_dataset_version.layer_elements)
        sub_dataset_title = (
            layer_elements[0]["subdatasetTitle"]
            if layer_elements and len(layer_elements) == 1
            else dataset_id
        )
        dataset_by_version = groupby(
            distinct_dataset_versions, key=lambda x: x.dataset_version
        )
        versions = list(chain(map_reject_none(to_version, dataset_by_version)))
        product = ProductDatasetFromPortal(
            dataset_id=dataset_id,
            dataset_name=sub_dataset_title,
            versions=versions,
            raw_services=first_distinct_dataset_version.raw_services,
            layer_elements=first_distinct_dataset_version.layer_elements,
        )
        return product
    return None


def to_part(
    distinct_dataset_version: DistinctDatasetVersionPart,
) -> List[CopernicusMarineVersionPart]:
    mds_stac_services = mds_stac_to_services(distinct_dataset_version)
    if mds_stac_services:
        return [
            CopernicusMarineVersionPart(
                name=distinct_dataset_version.dataset_part,
                services=mds_stac_services,
            )
        ]
    else:
        return []


def to_version(
    datasets_by_version,
) -> Optional[CopernicusMarineDatasetVersion]:
    distinct_dataset_versions = list(datasets_by_version[1])

    if distinct_dataset_versions:
        parts = list(chain(*map(to_part, distinct_dataset_versions)))
        unique_parts: List[CopernicusMarineVersionPart] = []
        for part in parts:
            if part.name not in list(map(lambda x: x.name, unique_parts)):
                unique_parts.append(part)
        if len(parts) != len(unique_parts):
            dataset_id = distinct_dataset_versions[0].dataset_id
            logger.debug(
                f"WARNING: The dataset id '{dataset_id}' has many parts "
                f"with the same name. Only the first one has been chosen."
            )
        if parts:
            return CopernicusMarineDatasetVersion(
                label=distinct_dataset_versions[0].dataset_version,
                parts=unique_parts,
            )
    return None


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


def construct_unique_dataset(
    raw_service, groups_layers, stac_items
) -> List[DistinctDatasetVersionPart]:
    dataset_id_from_raw_service = raw_service[0]
    groups_layer = [
        group_layer
        for group_layer in groups_layers.items()
        if group_layer[0] == dataset_id_from_raw_service
    ]
    dataset_layer_elements = groups_layer[0][1] if groups_layer else []
    dataset_raw_services = raw_service[1]

    dataset_versions = []

    for stac_dataset_id, stac_items_values in stac_items.items():
        if stac_dataset_id.startswith(dataset_id_from_raw_service):
            (
                _,
                dataset_version,
                part,
            ) = get_version_and_part_from_full_dataset_id(stac_dataset_id)
            dataset_versions.append(
                DistinctDatasetVersionPart(
                    dataset_id=dataset_id_from_raw_service,
                    dataset_version=dataset_version,
                    dataset_part=part,
                    layer_elements=dataset_layer_elements,
                    raw_services=dataset_raw_services,
                    stac_items_values=stac_items_values,
                )
            )
    else:
        _, dataset_version, part = get_version_and_part_from_full_dataset_id(
            dataset_id_from_raw_service
        )
        dataset_versions.append(
            DistinctDatasetVersionPart(
                dataset_id=dataset_id_from_raw_service,
                dataset_version=dataset_version,
                dataset_part=part,
                layer_elements=dataset_layer_elements,
                raw_services=dataset_raw_services,
                stac_items_values=None,
            )
        )

    return dataset_versions


def to_datasets(
    raw_services: dict[str, dict[str, str]],
    layers: dict[str, dict[str, Any]],
    stac_items: dict,
) -> list[ProductDatasetFromPortal]:
    groups_layers = defaultdict(list)
    for layer in layers.values():
        subdataset_id = layer["subdatasetId"]
        groups_layers[subdataset_id].append(layer)

    distinct_dataset_versions = map(
        construct_unique_dataset,
        raw_services.items(),
        repeat(groups_layers),
        repeat(stac_items),
    )

    return sorted(
        map_reject_none(to_dataset, distinct_dataset_versions),
        key=lambda distinct_dataset: distinct_dataset.dataset_id,
    )


def _parse_portal_product(raw_product: dict[str, Any]) -> ProductFromPortal:
    return ProductFromPortal(
        title=raw_product["title"],
        product_id=raw_product["id"],
        thumbnail_url=raw_product["thumbnailUrl"],
        description=raw_product["abstract"],
        digital_object_identifier=raw_product["doi"],
        sources=raw_product["sources"],
        processing_level=(
            raw_product["processingLevel"]
            if "processingLevel" in raw_product
            else None
        ),
        production_center=raw_product["originatingCenter"],
        keywords=raw_product["keywords"],
        datasets=to_datasets(
            raw_product["services"],
            raw_product["layers"],
            raw_product["stacItems"],
        ),
    )


def _parse_portal_backend_products(
    connection: CatalogParserConnection,
) -> List[ProductFromPortal]:
    base_url = "https://data-be-prd.marine.copernicus.eu/api/datasets"
    session = get_configured_request_session()
    response = session.post(
        base_url,
        json={"size": 1000, "includeOmis": True},
    )
    assert response.ok, response.text
    raw_catalogue: dict[str, Any] = loads(response.text)

    nest_asyncio.apply()
    loop = asyncio.get_event_loop()

    results = loop.run_until_complete(
        _async_fetch_raw_products(raw_catalogue["datasets"].keys(), connection)
    )
    return list(map(_parse_portal_product, results))


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
