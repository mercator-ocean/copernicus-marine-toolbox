import json
from typing import Any

from copernicusmarine.core_functions import decorators, documentation_utils
from copernicusmarine.core_functions.deprecated import deprecated_python_option
from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
)
from copernicusmarine.core_functions.describe import describe_function
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)


@decorators.docstring_parameter(documentation_utils.DESCRIBE)
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
    {DESCRIBE_DESCRIPTION_HELP}

    Parameters
    ----------
    include_description : bool, optional
        {INCLUDE_DESCRIPTION_HELP}
    include_datasets : bool, optional
        {INCLUDE_DATASETS_HELP}
    include_keywords : bool, optional
        {INCLUDE_KEYWORDS_HELP}
    include_versions : bool, optional
        {INCLUDE_VERSIONS_HELP}
    include_all : bool, optional
        {INCLUDE_ALL_HELP}
    contains : list[str], optional
        {CONTAINS_HELP}
    max_concurrent_requests : int, optional
        {MAX_CONCURRENT_REQUESTS_HELP}
    disable_progress_bar : bool, optional
        {DISABLE_PROGRESS_BAR_HELP}

    Returns
    -------
    {DESCRIBE_RESPONSE_HELP}
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
