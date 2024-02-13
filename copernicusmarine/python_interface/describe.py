import json
from typing import Any

from copernicusmarine.core_functions.describe import describe_function
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)


@log_exception_and_exit
def describe(
    include_description: bool = False,
    include_datasets: bool = False,
    include_keywords: bool = False,
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
        include_description (bool, optional): Whether to include description for each item. Defaults to False.
        include_datasets (bool, optional): Whether to include dataset information. Defaults to False.
        include_keywords (bool, optional): Whether to include keywords for each item. Defaults to False.
        contains (list[str], optional): List of strings to filter items containing these values. Defaults to [].
        overwrite_metadata_cache (bool, optional): Whether to overwrite the metadata cache. Defaults to False.
        no_metadata_cache (bool, optional): Whether to skip using the metadata cache. Defaults to False.

    Returns:
        dict[str, Any]: A dictionary containing the retrieved metadata information.
    """  # noqa

    if not isinstance(contains, list):
        raise ValueError("contains must be of list type")

    catalogue_json = describe_function(
        include_description,
        include_datasets,
        include_keywords,
        contains,
        overwrite_metadata_cache,
        no_metadata_cache,
        disable_progress_bar,
        staging=staging,
    )
    catalogue = json.loads(catalogue_json)
    return catalogue
