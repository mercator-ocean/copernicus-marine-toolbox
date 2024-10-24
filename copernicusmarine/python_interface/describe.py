import json
from typing import Any

from copernicusmarine.core_functions.deprecated import deprecated_python_option
from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
)
from copernicusmarine.core_functions.describe import describe_function
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)


@deprecated_python_option(DEPRECATED_OPTIONS)
@log_exception_and_exit
def describe(
    include_description: bool = False,
    include_datasets: bool = False,
    include_keywords: bool = False,
    include_versions: bool = False,
    include_all: bool = False,
    contains: list[str] = [],
    overwrite_metadata_cache: bool = False,
    no_metadata_cache: bool = False,
    disable_progress_bar: bool = False,
    staging: bool = False,
) -> dict[str, Any]:
    """
    Retrieve metadata information from the Copernicus Marine catalogue.

    This function fetches metadata information from the Copernicus Marine catalogue
    based on specified parameters and options.

    Args:
        include_description (bool, optional): Whether to include description for each product. Defaults to False.
        include_datasets (bool, optional): Whether to include dataset information. Defaults to False.
        include_keywords (bool, optional): Whether to include keywords for each product. Defaults to False.
        include_versions (bool, optional): Whether to include all versions of each dataset. Defaults to False.
        include_all (bool, optional): Whether to include all metadata information. Defaults to False.
        contains (list[str], optional): List of strings to filter items containing these values. Defaults to [].
        overwrite_metadata_cache (bool, optional): Whether to overwrite the metadata cache. Defaults to False.
        no_metadata_cache (bool, optional): Whether to skip using the metadata cache. Defaults to False.

    Returns:
        dict[str, Any]: A dictionary containing the retrieved metadata information.
    """  # noqa

    if not isinstance(contains, list):
        raise ValueError("contains must be of list type")

    if include_all:
        include_description = True
        include_datasets = True
        include_keywords = True
        include_versions = True

    catalogue_json = describe_function(
        include_description,
        include_datasets,
        include_keywords,
        include_versions,
        contains,
        overwrite_metadata_cache,
        no_metadata_cache,
        disable_progress_bar,
        staging=staging,
    )
    catalogue = json.loads(catalogue_json)
    return catalogue
