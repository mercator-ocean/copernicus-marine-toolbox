import logging
from typing import Optional

from copernicusmarine.catalogue_parser.catalogue_parser import (
    filter_catalogue_with_strings,
    parse_catalogue,
)
from copernicusmarine.catalogue_parser.models import CopernicusMarineCatalogue
from copernicusmarine.core_functions.versions_verifier import VersionVerifier

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

    VersionVerifier.check_version_describe(staging)
    if staging:
        logger.warning(
            "Detecting staging flag for describe command. "
            "Data will come from the staging environment."
        )

    base_catalogue: CopernicusMarineCatalogue = parse_catalogue(
        force_product_id=force_product_id,
        force_dataset_id=force_dataset_id,
        max_concurrent_requests=max_concurrent_requests,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )
    if not show_all_versions:
        base_catalogue.filter_only_official_versions_and_parts()

    response_catalogue = (
        filter_catalogue_with_strings(base_catalogue, set(contains))
        if contains
        else base_catalogue
    )
    return response_catalogue
