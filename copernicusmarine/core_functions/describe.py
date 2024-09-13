import json
import logging

from copernicusmarine.catalogue_parser.catalogue_parser import (
    filter_catalogue_with_strings,
    parse_catalogue,
)
from copernicusmarine.catalogue_parser.models import (
    CopernicusMarineCatalogue,
    CopernicusMarineDatasetServiceType,
)
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

    catalogue_dict = (
        filter_catalogue_with_strings(base_catalogue, contains)
        if contains
        else base_catalogue.__dict__
    )

    def default_filter(obj):
        if isinstance(obj, CopernicusMarineDatasetServiceType):
            return obj.to_json_dict()

        attributes = obj.__dict__
        attributes.pop("__objclass__", None)
        if not include_description:
            attributes.pop("description", None)
        if not include_datasets:
            attributes.pop("datasets", None)
        if not include_keywords:
            attributes.pop("keywords", None)
        return obj.__dict__

    json_dump = json.dumps(
        catalogue_dict, default=default_filter, sort_keys=False, indent=2
    )
    return json_dump
