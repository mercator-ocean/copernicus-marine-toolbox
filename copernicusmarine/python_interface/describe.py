from typing import Optional

from copernicusmarine.catalogue_parser.models import CopernicusMarineCatalogue
from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
    deprecated_python_option,
)
from copernicusmarine.core_functions.describe import describe_function
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)


@deprecated_python_option(**DEPRECATED_OPTIONS.dict_old_names_to_new_names)
@log_exception_and_exit
def describe(
    include_description: bool = False,
    something: bool = False,
    include_datasets: bool = False,
    include_keywords: bool = False,
    include_versions: bool = False,
    include_all: bool = False,
    contains: list[str] = [],
    product_id: Optional[str] = None,
    dataset_id: Optional[str] = None,
    max_concurrent_requests: int = 15,
    disable_progress_bar: bool = False,
    staging: bool = False,
) -> CopernicusMarineCatalogue:
    """
    Retrieve and parse the metadata information from the Copernicus Marine catalogue.

    Parameters
    ----------
    include_description : bool, optional
        Deprecated. Include product description in output.
    include_datasets : bool, optional
        Deprecated. Include product dataset details in output.
    include_keywords : bool, optional
        Deprecated. Include product keyword details in output.
    include_versions : bool, optional
        Include dataset versions in output. By default, shows only the default version.
    include_all : bool, optional
        Deprecated. Include all the possible data in output: description, datasets, keywords, and versions.
    contains : list[str], optional
        Filter catalogue output. Returns products with attributes matching a string token.
    product_id : str, optional
        Force the productID to be used for the describe command. Will not parse the whole catalogue, but only the product with the given productID.
    dataset_id : str, optional
        Force the datasetID to be used for the describe command. Will not parse the whole catalogue, but only the dataset with the given datasetID.
    max_concurrent_requests : int, optional
        Maximum number of concurrent requests (>=1). Default 15. The command uses a thread pool executor to manage concurrent requests.
    disable_progress_bar : bool, optional
        Flag to hide progress bar.

    Returns
    -------
    copernicusmarine.CopernicusMarineCatalogue
        An object containing the retrieved metadata information.

    """  # noqa

    if not isinstance(contains, list):
        raise ValueError("contains must be of list type")

    return describe_function(
        include_versions,
        contains,
        product_id,
        dataset_id,
        max_concurrent_requests,
        disable_progress_bar,
        staging=staging,
    )
