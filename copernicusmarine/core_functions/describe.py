import json
import logging

from copernicusmarine.catalogue_parser.catalogue_parser import (
    CopernicusMarineCatalogue,
    CopernicusMarineDatasetServiceType,
    filter_catalogue_with_strings,
    parse_catalogue,
)
from copernicusmarine.core_functions.utils import (
    create_cache_directory,
    delete_cache_folder,
)
from copernicusmarine.core_functions.versions_verifier import VersionVerifier

logger = logging.getLogger("copernicus_marine_root_logger")


def describe_function(
    include_description: bool,
    include_datasets: bool,
    include_keywords: bool,
    contains: list[str],
    overwrite_metadata_cache: bool,
    no_metadata_cache: bool,
    disable_progress_bar: bool,
    staging: bool,
) -> str:
    VersionVerifier.check_version_describe(staging)
    if staging:
        logger.warning(
            "Detecting staging flag for describe command. "
            "Data will come from the staging environment."
        )

    if overwrite_metadata_cache:
        delete_cache_folder(quiet=True)

    if not no_metadata_cache:
        create_cache_directory()

    base_catalogue: CopernicusMarineCatalogue = parse_catalogue(
        no_metadata_cache=no_metadata_cache,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )
    # TODO: the typing of catalogue_dict is wrong, it can be a CopernicusMarineCatalogue
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
