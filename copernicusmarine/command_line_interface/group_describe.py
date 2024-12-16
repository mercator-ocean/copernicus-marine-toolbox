import logging
from typing import Optional

import click

from copernicusmarine.catalogue_parser.models import CopernicusMarineCatalogue
from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.command_line_interface.utils import tqdm_disable_option
from copernicusmarine.core_functions import documentation_utils
from copernicusmarine.core_functions.click_custom_class import (
    CustomClickOptionsCommand,
)
from copernicusmarine.core_functions.describe import describe_function
from copernicusmarine.core_functions.fields_query_builder import (
    build_query,
    get_queryable_requested_fields,
)

logger = logging.getLogger("copernicusmarine")
blank_logger = logging.getLogger("copernicusmarine_blank_logger")


@click.group()
def cli_describe() -> None:
    pass


@cli_describe.command(
    "describe",
    cls=CustomClickOptionsCommand,
    short_help="Print products metadata of Copernicus Marine catalogue as JSON.",
    help=documentation_utils.DESCRIBE["DESCRIBE_DESCRIPTION_HELP"]
    + " \n\nReturns\n "
    + documentation_utils.DESCRIBE["DESCRIBE_RESPONSE_HELP"],  # noqa
    epilog="""
    Examples:

    .. code-block:: bash

        copernicusmarine describe --contains METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2 --return-fields datasets

    .. code-block:: bash

        copernicusmarine describe -c METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2 \n
    """,  # noqa
)
@click.option(
    "--show-all-versions",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.DESCRIBE["SHOW_ALL_VERSIONS_HELP"],
)
@click.option(
    "--return-fields",
    "-r",
    type=str,
    default=None,
    help=documentation_utils.DESCRIBE["RETURN_FIELDS_HELP"],
)
@click.option(
    "--exclude-fields",
    "-e",
    type=str,
    default=None,
    help=documentation_utils.DESCRIBE["EXCLUDE_FIELDS_HELP"],
)
@click.option(
    "--contains",
    "-c",
    type=str,
    multiple=True,
    help=documentation_utils.DESCRIBE["CONTAINS_HELP"],
)
@click.option(
    "--product-id",
    "-p",
    type=str,
    default=None,
    help=documentation_utils.DESCRIBE["PRODUCT_ID_HELP"],
)
@click.option(
    "--dataset-id",
    "-i",
    type=str,
    default=None,
    help=documentation_utils.DESCRIBE["DATASET_ID_HELP"],
)
@click.option(
    "--max-concurrent-requests",
    type=int,
    default=15,
    help=documentation_utils.DESCRIBE["MAX_CONCURRENT_REQUESTS_HELP"],
)
@tqdm_disable_option
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "QUIET"]),
    default="INFO",
    help=documentation_utils.DESCRIBE["LOG_LEVEL_HELP"],
)
@click.option(
    "--staging",
    type=bool,
    default=False,
    is_flag=True,
    hidden=True,
)
@log_exception_and_exit
def describe(
    show_all_versions: bool,
    return_fields: Optional[str],
    exclude_fields: Optional[str],
    contains: list[str],
    product_id: Optional[str],
    dataset_id: Optional[str],
    max_concurrent_requests: int,
    disable_progress_bar: bool,
    log_level: str,
    staging: bool,
) -> None:
    if log_level == "QUIET":
        logger.disabled = True
        logger.setLevel(level="CRITICAL")
    else:
        logger.setLevel(level=log_level)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("DEBUG mode activated")

    response_catalogue = describe_function(
        show_all_versions,
        contains,
        product_id,
        dataset_id,
        max_concurrent_requests,
        disable_progress_bar,
        staging,
    )
    include_query, exclude_query = _create_include_and_exclude(
        return_fields,
        exclude_fields,
    )
    blank_logger.info(
        response_catalogue.model_dump_json(
            exclude_unset=True,
            exclude_none=True,
            exclude=exclude_query,
            include=include_query,
            indent=2,
            context={"sort_keys": False},
        )
    )


def _create_include_and_exclude(
    return_fields: Optional[str],
    exclude_fields: Optional[str],
) -> tuple[Optional[dict], Optional[dict]]:
    include_query = None
    exclude_query = None
    if return_fields:
        include_in_output = set(return_fields.replace(" ", "").split(","))
        if "all" not in include_in_output:
            queryable_fields = get_queryable_requested_fields(
                include_in_output,
                CopernicusMarineCatalogue,
                "--return-fields",
            )
            include_query = build_query(
                queryable_fields, CopernicusMarineCatalogue
            )
    if exclude_fields:
        exclude_from_output = set(exclude_fields.replace(" ", "").split(","))
        queryable_fields = get_queryable_requested_fields(
            exclude_from_output, CopernicusMarineCatalogue, "--exclude-fields"
        )
        exclude_query = build_query(
            queryable_fields, CopernicusMarineCatalogue
        )

    return include_query, exclude_query
