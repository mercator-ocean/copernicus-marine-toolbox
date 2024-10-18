import logging

import click

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.command_line_interface.utils import tqdm_disable_option
from copernicusmarine.core_functions import documentation_utils
from copernicusmarine.core_functions.click_custom_class import (
    CustomClickOptionsCommand,
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

        copernicusmarine describe --contains METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2 --include-datasets

    .. code-block:: bash

        copernicusmarine describe -c METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2 \n
    """,  # noqa
)
@click.option(
    "--include-description",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.DESCRIBE["INCLUDE_DESCRIPTION_HELP"],
)
@click.option(
    "--include-datasets",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.DESCRIBE["INCLUDE_DATASETS_HELP"],
)
@click.option(
    "--include-keywords",
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
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.DESCRIBE["INCLUDE_ALL_HELP"],
)
@click.option(
    "--contains",
    "-c",
    type=str,
    multiple=True,
    help=documentation_utils.DESCRIBE["CONTAINS_HELP"],
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
    contains: list[str],
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
        include_description = True
        include_datasets = True
        include_keywords = True
        include_versions = True

    json_dump = describe_function(
        include_description=include_description,
        include_datasets=include_datasets,
        include_keywords=include_keywords,
        include_versions=include_versions,
        contains=contains,
        max_concurrent_requests=max_concurrent_requests,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )
    blank_logger.info(json_dump)
