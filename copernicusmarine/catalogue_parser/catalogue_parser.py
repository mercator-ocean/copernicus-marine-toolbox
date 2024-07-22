import asyncio
import logging
from dataclasses import dataclass
from enum import Enum
from itertools import groupby
from typing import Any, Dict, Iterator, List, Optional, Tuple

import nest_asyncio
import pystac
from aiohttp import ContentTypeError, ServerDisconnectedError
from tqdm import tqdm

from copernicusmarine.aioretry import RetryInfo, RetryPolicyStrategy, retry
from copernicusmarine.catalogue_parser.dataset_product_mapping import (
    dataset_product_mapping,
)
from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineCatalogue,
    CopernicusMarineProduct,
    CopernicusMarineProductDataset,
    get_version_and_part_from_full_dataset_id,
)
from copernicusmarine.core_functions.environment_variables import (
    COPERNICUSMARINE_MAX_CONCURRENT_REQUESTS,
)
from copernicusmarine.core_functions.sessions import (
    get_configured_aiohttp_session,
    get_https_proxy,
)
from copernicusmarine.core_functions.utils import (
    construct_query_params_for_marine_data_store_monitoring,
    map_reject_none,
    rolling_batch_gather,
)

logger = logging.getLogger("copernicusmarine")


MARINE_DATA_STORE_STAC_BASE_URL = (
    "https://s3.waw3-1.cloudferro.com/mdl-metadata/metadata"
)
MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL = (
    MARINE_DATA_STORE_STAC_BASE_URL + "/catalog.stac.json"
)
MARINE_DATA_STORE_STAC_BASE_URL_STAGING = (
    "https://s3.waw3-1.cloudferro.com/mdl-metadata-dta/metadata"
)
MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL_STAGING = (
    MARINE_DATA_STORE_STAC_BASE_URL_STAGING + "/catalog.stac.json"
)

MAX_CONCURRENT_REQUESTS = int(COPERNICUSMARINE_MAX_CONCURRENT_REQUESTS)


class CatalogParserConnection:
    def __init__(self, proxy: Optional[str] = None) -> None:
        self.proxy = proxy
        self.session = get_configured_aiohttp_session()
        self.proxy = get_https_proxy()
        self.__max_retries = 5
        self.__sleep_time = 1

    @retry("_retry_policy")
    async def get_json_file(self, url: str) -> dict[str, Any]:
        logger.debug(f"Fetching json file at this url: {url}")
        async with self.session.get(
            url,
            params=construct_query_params_for_marine_data_store_monitoring(),
            proxy=self.proxy,
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
                ServerDisconnectedError,
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


async def get_dataset_metadata(
    dataset_id: str, staging: bool
) -> Optional[CopernicusMarineProductDataset]:
    connection = CatalogParserConnection()
    product_id = dataset_product_mapping[dataset_id]  # here mds mapping
    root_url = (
        MARINE_DATA_STORE_STAC_BASE_URL
        if not staging
        else MARINE_DATA_STORE_STAC_BASE_URL_STAGING
    )
    url = f"{root_url}/{product_id}/product.stac.json"
    product_json = await connection.get_json_file(url)
    product_collection = pystac.Collection.from_dict(product_json)
    product_datasets_metadata_links = product_collection.get_item_links()
    datasets_metadata_links = [
        dataset_metadata_link
        for dataset_metadata_link in product_datasets_metadata_links
        if dataset_id in dataset_metadata_link.href
    ]
    if not datasets_metadata_links:
        return None
    # TODO: check if we gain a lot of time by doing a gather here
    # if not don't forget to add a retry policy

    dataset_jsons: list[dict] = await asyncio.gather(
        *[
            connection.get_json_file(f"{root_url}/{product_id}/{link.href}")
            for link in datasets_metadata_links
        ]
    )
    await connection.close()
    dataset_items = [
        dataset_item
        for dataset_json in dataset_jsons
        if (dataset_item := _parse_dataset_json_to_pystac_item(dataset_json))
    ]
    return _parse_and_sort_dataset_items(dataset_items)


def _parse_dataset_json_to_pystac_item(
    metadate_json: dict,
) -> Optional[pystac.Item]:
    try:
        return pystac.Item.from_dict(metadate_json)
    except pystac.STACError as exception:
        message = (
            "Invalid Item: If datetime is None, a start_datetime "
            + "and end_datetime must be supplied."
        )
        if exception.args[0] != message:
            logger.error(exception)
            raise pystac.STACError(exception.args)
    return None


def _parse_product_json_to_pystac_collection(
    metadata_json: dict,
) -> Optional[pystac.Collection]:
    try:
        return pystac.Collection.from_dict(metadata_json)
    except KeyError as exception:
        messages = ["spatial", "temporal"]
        if exception.args[0] not in messages:
            logger.error(exception)
        return None


def _parse_and_sort_dataset_items(
    dataset_items: list[pystac.Item],
) -> Optional[CopernicusMarineProductDataset]:
    """
    Return all dataset metadata parsed and sorted.
    The first version and part are the default.
    """
    dataset_item_example = dataset_items[0]
    dataset_id, _, _ = get_version_and_part_from_full_dataset_id(
        dataset_item_example.id
    )
    dataset_part_version_merged = CopernicusMarineProductDataset(
        dataset_id=dataset_id,
        dataset_name=dataset_item_example.properties.get("title", dataset_id),
        versions=[],
    )
    dataset_part_version_merged.parse_dataset_metadata_items(dataset_items)

    if dataset_part_version_merged.versions == []:
        return None

    dataset_part_version_merged.sort_versions()
    return dataset_part_version_merged


def _construct_marine_data_store_product(
    stac_tuple: Tuple[pystac.Collection, List[pystac.Item]],
) -> CopernicusMarineProduct:
    stac_product, stac_datasets = stac_tuple
    stac_datasets_sorted = sorted(stac_datasets, key=lambda x: x.id)
    dataset_items_by_dataset_id = groupby(
        stac_datasets_sorted,
        key=lambda x: get_version_and_part_from_full_dataset_id(x.id)[0],
    )

    datasets = [
        dataset_metadata
        for _, dataset_items in dataset_items_by_dataset_id
        if (
            dataset_metadata := _parse_and_sort_dataset_items(
                list(dataset_items)
            )
        )
    ]

    production_center = [
        provider.name
        for provider in stac_product.providers or []
        if provider.roles and "producer" in provider.roles
    ]

    production_center_name = production_center[0] if production_center else ""

    if stac_product.assets:
        thumbnail = stac_product.assets.get("thumbnail")
        if thumbnail:
            thumbnail_url = thumbnail.get_absolute_href()
        else:
            thumbnail_url = None
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

    return CopernicusMarineProduct(
        title=stac_product.title or stac_product.id,
        product_id=stac_product.id,
        thumbnail_url=thumbnail_url or "",
        description=stac_product.description,
        digital_object_identifier=digital_object_identifier,
        sources=sources,
        processing_level=processing_level,
        production_center=production_center_name,
        keywords=stac_product.keywords,
        datasets=datasets,
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


async def async_fetch_dataset_items(
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
        item_json = await connection.get_json_file(url)
        item = _parse_dataset_json_to_pystac_item(item_json)
        if item:
            items.append(item)
    return items


async def async_fetch_collection(
    root_url: str,
    connection: CatalogParserConnection,
    url: str,
) -> Optional[Tuple[pystac.Collection, List[pystac.Item]]]:
    json_collection = await connection.get_json_file(url)
    collection = _parse_product_json_to_pystac_collection(json_collection)
    if collection:
        items = await async_fetch_dataset_items(
            root_url, connection, collection
        )
        return (collection, items)
    return None


async def async_fetch_product_items(
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


async def async_fetch_all_products_items(
    connection: CatalogParserConnection,
    staging: bool,
) -> Iterator[Optional[tuple[pystac.Collection, list[pystac.Item]]]]:
    catalog_root_url = (
        MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL
        if not staging
        else MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL_STAGING
    )
    json_catalog = await connection.get_json_file(catalog_root_url)
    catalog = pystac.Catalog.from_dict(json_catalog)
    catalog.set_self_href(catalog_root_url)
    child_links = catalog.get_child_links()
    root_url = (
        MARINE_DATA_STORE_STAC_BASE_URL
        if not staging
        else (MARINE_DATA_STORE_STAC_BASE_URL_STAGING)
    )
    childs = await async_fetch_product_items(root_url, connection, child_links)
    return childs


def parse_catalogue(
    disable_progress_bar: bool,
    staging: bool = False,
) -> CopernicusMarineCatalogue:
    logger.debug("Parsing catalogue...")
    progress_bar = tqdm(
        total=2, desc="Fetching catalog", disable=disable_progress_bar
    )

    connection = CatalogParserConnection()
    nest_asyncio.apply()
    loop = asyncio.get_event_loop()
    marine_data_store_root_collections = loop.run_until_complete(
        async_fetch_all_products_items(connection=connection, staging=staging)
    )
    progress_bar.update()

    products_metadata = [
        product_metadata
        for product_item in marine_data_store_root_collections
        if product_item
        and (
            (
                product_metadata := _construct_marine_data_store_product(
                    product_item
                )
            ).datasets
        )
    ]
    products_metadata.sort(key=lambda x: x.product_id)

    full_catalog = CopernicusMarineCatalogue(products=products_metadata)

    loop.run_until_complete(connection.close())

    progress_bar.update()
    logger.debug("Catalogue parsed")
    return full_catalog


@dataclass
class DistinctDatasetVersionPart:
    dataset_id: str
    dataset_version: str
    dataset_part: str
    layer_elements: List
    raw_services: Dict
    stac_items_values: Optional[Dict]


# ---------------------------------------
# --- Utils function on any catalogue ---
# ---------------------------------------


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
