import logging
from typing import Any

from copernicusmarine.catalogue_parser.catalogue_parser import (
    filter_catalogue_with_strings,
    parse_catalogue,
)
from copernicusmarine.catalogue_parser.models import CopernicusMarineCatalogue
from copernicusmarine.core_functions.versions_verifier import VersionVerifier

logger = logging.getLogger("copernicusmarine")


def describe_function(
    include_description: bool,
    include_datasets: bool,
    include_keywords: bool,
    include_versions: bool,
    contains: list[str],
    max_concurrent_requests: int,
    disable_progress_bar: bool,
    staging: bool,
) -> str:

    VersionVerifier.check_version_describe(staging)
    if staging:
        logger.warning(
            "Detecting staging flag for describe command. "
            "Data will come from the staging environment."
        )

    base_catalogue: CopernicusMarineCatalogue = parse_catalogue(
        max_concurrent_requests=max_concurrent_requests,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )
    if not include_versions:
        base_catalogue.filter_only_official_versions_and_parts()
    to_exclude: dict[str, Any] = {"products": {"__all__": set()}}
    if not include_datasets:
        to_exclude["products"]["__all__"].add("datasets")
    if not include_keywords:
        to_exclude["products"]["__all__"].add("keywords")
    if not include_description:
        to_exclude["products"]["__all__"].add("description")

    response_catalogue = (
        filter_catalogue_with_strings(base_catalogue, set(contains))
        if contains
        else base_catalogue
    )

    return response_catalogue.model_dump_json(
        exclude_unset=True,
        exclude_none=True,
        exclude=to_exclude,
        indent=2,
        context={"sort_keys": False},
    )
