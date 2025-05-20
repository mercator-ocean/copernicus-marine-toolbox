import logging
from typing import Optional

from copernicusmarine.catalogue_parser.catalogue_parser import (
    filter_catalogue_with_strings,
    merge_catalogues,
    parse_catalogue,
)
from copernicusmarine.catalogue_parser.models import CopernicusMarineCatalogue
from copernicusmarine.core_functions.marine_datastore_config import (
    get_config_and_check_version_describe,
)

logger = logging.getLogger("copernicusmarine")


def describe_function(
    show_all_versions: bool,
    contains: list[str],
    force_product_id: Optional[str],
    force_dataset_id: Optional[str],
    max_concurrent_requests: int,
    disable_progress_bar: bool,
    staging: bool,
) -> CopernicusMarineCatalogue:
    marine_datasetore_config = get_config_and_check_version_describe(staging)
    if staging:
        logger.warning(
            "Detecting staging flag for describe command. "
            "Data will come from the staging environment."
        )
    catalogues: list[CopernicusMarineCatalogue] = []
    for catalogue_config in marine_datasetore_config.catalogues:
        catalogue: CopernicusMarineCatalogue = parse_catalogue(
            force_product_id=force_product_id,
            force_dataset_id=force_dataset_id,
            max_concurrent_requests=max_concurrent_requests,
            disable_progress_bar=disable_progress_bar,
            catalogue_config=catalogue_config,
        )
        catalogues.append(catalogue)

    # Merge all catalogues
    base_catalogue = merge_catalogues(catalogues)

    if not show_all_versions:
        base_catalogue.filter_only_official_versions_and_parts()

    response_catalogue = (
        filter_catalogue_with_strings(base_catalogue, set(contains))
        if contains
        else base_catalogue
    )
    return response_catalogue
