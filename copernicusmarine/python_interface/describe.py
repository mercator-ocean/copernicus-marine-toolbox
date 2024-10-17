import json
from typing import Any

from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
)
from copernicusmarine.core_functions.describe import describe_function
from copernicusmarine.core_functions.utils import deprecated_python_option
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)


@deprecated_python_option(**DEPRECATED_OPTIONS.dict_old_names_to_new_names)
@log_exception_and_exit
def describe(
    include_description: bool = False,
    include_datasets: bool = False,
    include_keywords: bool = False,
    include_versions: bool = False,
    include_all: bool = False,
    contains: list[str] = [],
    max_concurrent_requests: int = 15,
    disable_progress_bar: bool = False,
    staging: bool = False,
) -> dict[str, Any]:
    """
    Retrieve and parse the metadata information from the Copernicus Marine catalogue.

    Parameters
    ----------
    include_description : bool, optional
        Include product description in output.
    include_datasets : bool, optional
        Include product dataset details in output.
    include_keywords : bool, optional
        Include product keyword details in output.
    include_versions : bool, optional
        Include dataset versions in output. By default, shows only the default version.
    include_all : bool, optional
        Include all the possible data in output: description, datasets, keywords, and versions.
    contains : list[str], optional
        Filter catalogue output. Returns products with attributes matching a string token.
    max_concurrent_requests : int, optional
        Maximum number of concurrent requests (>=1). Default 15. The command uses a thread pool executor to manage concurrent requests.
    disable_progress_bar : bool, optional
        Flag to hide progress bar.

    Returns
    -------
    dict[str, Any]
        A dictionary containing the retrieved metadata information.

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
        max_concurrent_requests,
        disable_progress_bar,
        staging=staging,
    )
    catalogue = json.loads(catalogue_json)
    return catalogue
