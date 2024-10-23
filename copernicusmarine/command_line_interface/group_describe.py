import logging
from typing import Optional

import click

from copernicusmarine.catalogue_parser.fields_query_builder import QueryBuilder
from copernicusmarine.catalogue_parser.models import CopernicusMarineCatalogue
from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.command_line_interface.utils import tqdm_disable_option
from copernicusmarine.core_functions import documentation_utils
from copernicusmarine.core_functions.click_custom_class import (
    CustomClickOptionsCommand,
    DeprecatedClickOption,
)
from copernicusmarine.core_functions.describe import describe_function

logger = logging.getLogger("copernicusmarine")
blank_logger = logging.getLogger("copernicusmarine_blank_logger")


@click.group()
def cli_describe() -> None:
    pass


@cli_describe.command(
    "describe",
    cls=CustomClickOptionsCommand,
    short_help="Print Copernicus Marine catalogue as JSON.",
    help=documentation_utils.DESCRIBE["DESCRIBE_DESCRIPTION_HELP"]
    + " \n\nReturns\n "
    + documentation_utils.DESCRIBE["DESCRIBE_RESPONSE_HELP"],  # noqa
    epilog="""
    Examples:

    .. code-block:: bash

        copernicusmarine describe --contains METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2 --returned-fields datasets

    .. code-block:: bash

        copernicusmarine describe -c METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2 \n
    """,  # noqa
)
@click.option(
    "--include-description",
    cls=DeprecatedClickOption,
    deprecated=["--include-description"],
    preferred="--returned-fields description",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.DESCRIBE["INCLUDE_DESCRIPTION_HELP"],
)
@click.option(
    "--include-datasets",
    cls=DeprecatedClickOption,
    deprecated=["--include-datasets"],
    preferred="--returned-fields datasets",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.DESCRIBE["INCLUDE_DATASETS_HELP"],
)
@click.option(
    "--include-keywords",
    cls=DeprecatedClickOption,
    deprecated=["--include-keywords"],
    preferred="--returned-fields keywords",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.DESCRIBE["INCLUDE_KEYWORDS_HELP"],
)
@click.option(
    "--include-versions",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.DESCRIBE["INCLUDE_VERSIONS_HELP"],
)
@click.option(
    "-a",
    "--include-all",
    cls=DeprecatedClickOption,
    deprecated=["--include-all"],
    preferred="--returned-fields all",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.DESCRIBE["INCLUDE_ALL_HELP"],
)
@click.option(
    "--returned-fields",
    "-r",
    type=str,
    default=None,
    help=documentation_utils.DESCRIBE["RETURNED_FIELDS_HELP"],
)
@click.option(
    "--returned-fields-exclude",
    "-e",
    type=str,
    default=None,
    help=documentation_utils.DESCRIBE["RETURNED_FIELDS_EXCLUDE_HELP"],
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
    include_description: bool,
    include_datasets: bool,
    include_keywords: bool,
    include_versions: bool,
    include_all: bool,
    returned_fields: Optional[str],
    returned_fields_exclude: Optional[str],
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

    if include_all:
        include_versions = True

    response_catalogue = describe_function(
        include_versions,
        contains,
        product_id,
        dataset_id,
        max_concurrent_requests,
        disable_progress_bar,
        staging,
    )
    include_query, exclude_query = _create_include_and_exclude(
        returned_fields,
        returned_fields_exclude,
        include_datasets,
        include_keywords,
        include_description,
        include_all,
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
    returned_fields: Optional[str],
    returned_fields_exclude: Optional[str],
    include_datasets: bool,
    include_keywords: bool,
    include_description: bool,
    include_all: bool,
) -> tuple[Optional[dict], Optional[dict]]:

    if include_all:
        include_description = True
        include_datasets = True
        include_keywords = True
    include_in_output = set()
    if returned_fields:
        include_in_output = set(returned_fields.replace(" ", "").split(","))
    exclude_from_output = set()
    if returned_fields_exclude:
        exclude_from_output = set(
            returned_fields_exclude.replace(" ", "").split(",")
        )
    if (
        not include_datasets
        and not exclude_from_output
        and not include_in_output
        and ("datasets" not in include_in_output)
    ):
        exclude_from_output.add("datasets")
    if (
        not include_keywords
        and not exclude_from_output
        and not include_in_output
        and ("keywords" not in include_in_output)
    ):
        exclude_from_output.add("keywords")
    if (
        not include_description
        and not exclude_from_output
        and not include_in_output
        and ("description" not in include_in_output)
    ):
        exclude_from_output.add("description")

    include_query = None
    if include_in_output and "all" not in include_in_output:
        query_builder = QueryBuilder(include_in_output)
        include_query = query_builder.build_query(CopernicusMarineCatalogue)
    exclude_query = None
    if exclude_from_output:
        query_builder = QueryBuilder(exclude_from_output)
        exclude_query = query_builder.build_query(CopernicusMarineCatalogue)
    return include_query, exclude_query
