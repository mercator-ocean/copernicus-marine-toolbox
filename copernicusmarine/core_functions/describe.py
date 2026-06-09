import logging

from copernicusmarine.catalogue_parser.catalogue_parser import (
    filter_catalogue_with_strings,
    merge_catalogues,
    parse_catalogue,
)
from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineCatalogue,
    DatasetNotFound,
    ProductNotFound,
    get_version_from_dataset_id,
)
from copernicusmarine.core_functions.marine_datastore_config import (
    get_config_and_check_version_describe,
)

logger = logging.getLogger("copernicusmarine")


def describe_function(
    show_all_versions: bool,
    contains: list[str],
    force_product_id: str | None,
    force_dataset_id: str | None,
    max_concurrent_requests: int,
    disable_progress_bar: bool,
    staging: bool,
    raise_on_error: bool,
) -> CopernicusMarineCatalogue:
    marine_datasetore_config = get_config_and_check_version_describe(staging)
    if staging:
        logger.warning(
            "Detecting staging flag for describe command. "
            "Data will come from the staging environment."
        )
    catalogues: list[CopernicusMarineCatalogue] = []

    if force_dataset_id:
        force_dataset_id = _parse_dataset_id_and_version(force_dataset_id)
    for i, catalogue_config in enumerate(marine_datasetore_config.catalogues):
        try:
            catalogue: CopernicusMarineCatalogue | None = parse_catalogue(
                force_product_id=force_product_id,
                force_dataset_id=force_dataset_id,
                max_concurrent_requests=max_concurrent_requests,
                disable_progress_bar=disable_progress_bar,
                catalogue_config=catalogue_config,
                catalogue_number=i,
                raise_on_error=raise_on_error,
            )
            if catalogue:
                catalogues.append(catalogue)
        except DatasetNotFound:
            logger.debug(
                f"Dataset not found in catalogue {catalogue_config.root_metadata_url}"
            )
    # Merge all catalogues
    base_catalogue = merge_catalogues(catalogues)
    if (force_dataset_id or force_product_id) and not base_catalogue.products:
        if force_product_id:
            raise ProductNotFound(
                product_id=force_product_id,
            )
        if force_dataset_id:
            raise DatasetNotFound(
                dataset_id=force_dataset_id,
            )
    if not show_all_versions:
        base_catalogue.filter_only_official_versions_and_parts()
    response_catalogue = (
        filter_catalogue_with_strings(base_catalogue, set(contains))
        if contains
        else base_catalogue
    )
    return response_catalogue


def _parse_dataset_id_and_version(dataset_id: str) -> str:
    dataset_id_without_version, dataset_version = get_version_from_dataset_id(
        dataset_id=dataset_id, raise_on_error=False
    )
    if dataset_version:
        logger.warning(
            "The dataset version has been included "
            "in the dataset_id argument. "
            "This is not recommended: "
            "the version suffix will be ignored. "
        )
    return dataset_id_without_version
