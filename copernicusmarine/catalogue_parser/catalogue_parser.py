import logging
from enum import Enum
from itertools import groupby
from typing import Any, Optional, Union

import pystac
from pydantic import BaseModel
from tqdm import tqdm

from copernicusmarine.catalogue_parser.models import (
    PART_DEFAULT,
    CopernicusMarineCatalogue,
    CopernicusMarineDataset,
    CopernicusMarineProduct,
    DatasetIsNotPartOfTheProduct,
    DatasetNotFound,
    get_version_and_part_from_full_dataset_id,
)
from copernicusmarine.core_functions.sessions import JsonParserConnection
from copernicusmarine.core_functions.utils import run_concurrently

logger = logging.getLogger("copernicusmarine")

MARINE_DATA_STORE_ROOT_METADATA_URL = (
    "https://s3.waw3-1.cloudferro.com/mdl-metadata"
)

MARINE_DATA_STORE_ROOT_METADATA_URL_STAGING = (
    "https://s3.waw3-1.cloudferro.com/mdl-metadata-dta"
)

MARINE_DATA_STORE_STAC_URL = f"{MARINE_DATA_STORE_ROOT_METADATA_URL}/metadata"
MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL = (
    MARINE_DATA_STORE_STAC_URL + "/catalog.stac.json"
)
MARINE_DATA_STORE_STAC_URL_STAGING = (
    f"{MARINE_DATA_STORE_ROOT_METADATA_URL_STAGING}/metadata"
)
MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL_STAGING = (
    MARINE_DATA_STORE_STAC_URL_STAGING + "/catalog.stac.json"
)


def get_dataset_metadata(
    dataset_id: str, staging: bool
) -> Optional[CopernicusMarineDataset]:
    with JsonParserConnection() as connection:
        stac_url = (
            MARINE_DATA_STORE_STAC_URL
            if not staging
            else MARINE_DATA_STORE_STAC_URL_STAGING
        )
        root_url = (
            MARINE_DATA_STORE_ROOT_METADATA_URL
            if not staging
            else MARINE_DATA_STORE_ROOT_METADATA_URL_STAGING
        )
        dataset_product_mapping_url = (
            f"{root_url}/dataset_product_id_mapping.json"
        )
        product_ids = connection.get_json_file(
            dataset_product_mapping_url
        ).get(dataset_id)
        if not product_ids:
            raise DatasetNotFound(dataset_id)
        url_dataset_jsons_mapping: dict[str, dict] = {}
        for product_id in product_ids.split(","):
            url = f"{stac_url}/{product_id}/product.stac.json"
            product_json = connection.get_json_file(url)
            product_collection = pystac.Collection.from_dict(product_json)
            product_datasets_metadata_links = (
                product_collection.get_item_links()
            )
            datasets_metadata_links = [
                dataset_metadata_link
                for dataset_metadata_link in product_datasets_metadata_links
                if dataset_id in dataset_metadata_link.href
            ]
            if not datasets_metadata_links:
                continue
            url_dataset_jsons_mapping.update(
                {
                    f"{stac_url}/{product_id}/{link.href}": connection.get_json_file(
                        f"{stac_url}/{product_id}/{link.href}"
                    )
                    for link in datasets_metadata_links
                }
            )

        url_dataset_items_mapping = {
            url: dataset_item
            for url, dataset_json in url_dataset_jsons_mapping.items()
            if (
                dataset_item := _parse_dataset_json_to_pystac_item(
                    dataset_json
                )
            )
            and get_version_and_part_from_full_dataset_id(dataset_item.id)[0]
            == dataset_id
        }
        return _parse_and_sort_dataset_items(url_dataset_items_mapping)


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
    url_dataset_items_mapping: dict[str, pystac.Item],
) -> Optional[CopernicusMarineDataset]:
    """
    Return all dataset metadata parsed and sorted.
    The first version and part are the default.
    """
    dataset_item_example = list(url_dataset_items_mapping.values())[0]
    dataset_without_parts: list[tuple[pystac.Item, str]] = []
    for dataset_item in url_dataset_items_mapping.values():
        dataset_id, _, part = get_version_and_part_from_full_dataset_id(
            dataset_item.id
        )
        if part == PART_DEFAULT:
            dataset_without_parts.append((dataset_item, dataset_id))
            break
    if not dataset_without_parts:
        dataset_id, _, _ = get_version_and_part_from_full_dataset_id(
            dataset_item_example.id
        )
        dataset_title = dataset_id
    else:
        dataset_item, dataset_id = dataset_without_parts[0]
        dataset_title = dataset_item.properties.get("title", dataset_id)

    dataset_part_version_merged = CopernicusMarineDataset(
        dataset_id=dataset_id,
        dataset_name=dataset_title,
        versions=[],
    )
    dataset_part_version_merged.parse_dataset_metadata_items(
        url_dataset_items_mapping
    )

    if dataset_part_version_merged.versions == []:
        return None

    dataset_part_version_merged.sort_versions()
    return dataset_part_version_merged


def _construct_marine_data_store_product(
    stac_tuple: tuple[pystac.Collection, list[tuple[pystac.Item, str]]],
) -> CopernicusMarineProduct:
    stac_product, stac_datasets = stac_tuple
    stac_datasets_sorted = sorted(stac_datasets, key=lambda x: x[0].id)
    dataset_items_by_dataset_id = groupby(
        stac_datasets_sorted,
        key=lambda x: get_version_and_part_from_full_dataset_id(x[0].id)[0],
    )

    datasets = [
        dataset_metadata
        for _, dataset_items in dataset_items_by_dataset_id
        if (
            dataset_metadata := _parse_and_sort_dataset_items(
                {url_item: item for item, url_item in dataset_items}
            )
        )
    ]

    production_center = [
        provider.name
        for provider in stac_product.providers or []
        if provider.roles and "producer" in provider.roles
    ]

    production_center_name = production_center[0] if production_center else ""

    thumbnail_url = None
    if stac_product.assets:
        thumbnail = stac_product.assets.get("thumbnail")
        if thumbnail:
            thumbnail_url = thumbnail.get_absolute_href()

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
    properties: dict[str, str] = (
        stac_product.extra_fields.get("properties", {})
        if stac_product.extra_fields
        else {}
    )
    return properties.get(property_key)


def fetch_dataset_items(
    root_url: str,
    connection: JsonParserConnection,
    collection: pystac.Collection,
    force_dataset_id: Optional[str],
) -> list[tuple[pystac.Item, str]]:
    items = []
    for link in collection.get_item_links():
        if not link.owner:
            logger.warning(f"Invalid Item, no owner for: {link.href}")
            continue
        if force_dataset_id and force_dataset_id not in link.href:
            continue
        url = root_url + "/" + link.owner.id + "/" + link.href
        item_json = connection.get_json_file(url)
        item = _parse_dataset_json_to_pystac_item(item_json)
        if item:
            items.append((item, url))
    return items


def fetch_collection(
    root_url: str,
    connection: JsonParserConnection,
    url: str,
    force_dataset_id: Optional[str],
) -> Optional[tuple[pystac.Collection, list[tuple[pystac.Item, str]]]]:
    json_collection = connection.get_json_file(url)
    collection = _parse_product_json_to_pystac_collection(json_collection)
    if collection:
        items = fetch_dataset_items(
            root_url, connection, collection, force_dataset_id
        )
        return (collection, items)
    return None


def fetch_product_items(
    root_url: str,
    connection: JsonParserConnection,
    child_links: list[pystac.Link],
    force_product_id: Optional[str],
    force_dataset_id: Optional[str],
    max_concurrent_requests: int,
    disable_progress_bar: bool,
) -> list[Optional[tuple[pystac.Collection, list[tuple[pystac.Item, str]]]]]:
    tasks = []
    for link in child_links:
        if force_product_id and force_product_id not in link.href:
            continue
        tasks.append(
            (root_url, connection, link.absolute_href, force_dataset_id)
        )
    tdqm_bar_configuration = {
        "desc": "Fetching products",
        "disable": disable_progress_bar,
        "leave": False,
    }
    return [
        result
        for result in run_concurrently(
            fetch_collection,
            tasks,
            max_concurrent_requests,
            tdqm_bar_configuration,
        )
        if result is not None
    ]


def fetch_all_products_items(
    connection: JsonParserConnection,
    force_product_id: Optional[str],
    force_dataset_id: Optional[str],
    max_concurrent_requests: int,
    staging: bool,
    disable_progress_bar: bool,
) -> list[Optional[tuple[pystac.Collection, list[tuple[pystac.Item, str]]]]]:
    catalog_root_url = (
        MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL
        if not staging
        else MARINE_DATA_STORE_STAC_ROOT_CATALOG_URL_STAGING
    )
    json_catalog = connection.get_json_file(catalog_root_url)
    catalog = pystac.Catalog.from_dict(json_catalog)
    catalog.set_self_href(catalog_root_url)
    child_links = catalog.get_child_links()
    root_url = (
        MARINE_DATA_STORE_STAC_URL
        if not staging
        else (MARINE_DATA_STORE_STAC_URL_STAGING)
    )
    childs = fetch_product_items(
        root_url,
        connection,
        child_links,
        force_product_id,
        force_dataset_id,
        max_concurrent_requests,
        disable_progress_bar,
    )
    return childs


def parse_catalogue(
    force_product_id: Optional[str],
    force_dataset_id: Optional[str],
    max_concurrent_requests: int,
    disable_progress_bar: bool,
    staging: bool = False,
) -> CopernicusMarineCatalogue:
    logger.debug("Parsing catalogue...")
    progress_bar = tqdm(
        total=2, desc="Fetching catalogue", disable=disable_progress_bar
    )
    with JsonParserConnection() as connection:
        if force_dataset_id:
            root_url = (
                MARINE_DATA_STORE_ROOT_METADATA_URL
                if not staging
                else MARINE_DATA_STORE_ROOT_METADATA_URL_STAGING
            )
            dataset_product_mapping_url = (
                f"{root_url}/dataset_product_id_mapping.json"
            )
            product_id_from_mapping = connection.get_json_file(
                dataset_product_mapping_url
            ).get(force_dataset_id)
            if not product_id_from_mapping:
                raise DatasetNotFound(force_dataset_id)
            if (
                force_product_id
                and product_id_from_mapping != force_product_id
            ):
                raise DatasetIsNotPartOfTheProduct(
                    force_dataset_id, force_product_id
                )
            force_product_id = product_id_from_mapping
        marine_data_store_root_collections = fetch_all_products_items(
            connection=connection,
            force_product_id=force_product_id,
            force_dataset_id=force_dataset_id,
            max_concurrent_requests=max_concurrent_requests,
            staging=staging,
            disable_progress_bar=disable_progress_bar,
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

    progress_bar.update()
    logger.debug("Catalogue parsed")
    return full_catalog


# ---------------------------------------
# --- Utils functions
# ---------------------------------------


def search_and_filter(
    model: BaseModel, search_str: set[str]
) -> Union[BaseModel, None]:
    filtered_fields = {}
    search_str = {s.lower() for s in search_str}
    for field, value in model:
        if isinstance(value, BaseModel):
            filtered_value = search_and_filter(value, search_str)
            if filtered_value:
                filtered_fields[field] = filtered_value

        elif isinstance(value, list) or isinstance(value, tuple):
            filtered_list = []
            for item in value:
                if isinstance(item, BaseModel):
                    filtered_item = search_and_filter(item, search_str)
                    if filtered_item:
                        filtered_list.append(filtered_item)
                elif isinstance(item, str) and any(
                    s in item.lower() for s in search_str
                ):
                    filtered_list.append(item)

            if filtered_list and isinstance(value, list):
                filtered_fields[field] = filtered_list

            if filtered_list and isinstance(value, tuple):
                filtered_fields[field] = tuple(filtered_list)

        elif isinstance(value, dict):
            filtered_dict = {}
            for key, val in value.items():
                if isinstance(val, BaseModel):
                    filtered_val = search_and_filter(val, search_str)
                    if filtered_val:
                        filtered_dict[key] = filtered_val
                elif isinstance(val, str) and any(
                    s in val.lower() for s in search_str
                ):
                    filtered_dict[key] = val

            if filtered_dict:
                filtered_fields[field] = filtered_dict

        elif isinstance(value, Enum):
            if any(s in value.name.lower() for s in search_str):
                filtered_fields[field] = value

        elif isinstance(value, str) and any(
            s in value.lower() for s in search_str
        ):
            filtered_fields[field] = value
    if filtered_fields:
        return model.model_copy(update=filtered_fields)
    return None


def filter_catalogue_with_strings(
    catalogue: CopernicusMarineCatalogue, search_str: set[str]
) -> CopernicusMarineCatalogue:
    filtered_models = []
    for model in catalogue.products:
        filtered_model = search_and_filter(model, search_str)
        if filtered_model:
            filtered_models.append(filtered_model)
    return CopernicusMarineCatalogue(products=filtered_models)
