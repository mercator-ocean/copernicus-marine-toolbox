import logging

import click

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.command_line_interface.utils import (
    MutuallyExclusiveOption,
    tqdm_disable_option,
)
from copernicusmarine.core_functions.describe import describe_function

logger = logging.getLogger("copernicus_marine_root_logger")
blank_logger = logging.getLogger("copernicus_marine_blank_logger")


@click.group()
def cli_group_describe() -> None:
    pass


@cli_group_describe.command(
    "describe",
    short_help="Print Copernicus Marine catalog as JSON.",
    help="""
    Print Copernicus Marine catalog as JSON.

    The default display contains information on the products, and more data
    can be displayed using the --include-<argument> flags.

    The --contains option allows the user to specify one or several strings to
    filter through the catalogue display. The search is performed recursively
    on all attributes of the catalogue, and the tokens only need to be
    contained in one of the attributes (i.e. not exact match).
    """,
    epilog="""
    Examples:

    \b
    copernicusmarine describe --contains METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2 --include-datasets

    \b
    copernicusmarine describe -c METOFFICE-GLO-SST-L4-NRT-OBS-SST-V2
    """,  # noqa
)
@click.option(
    "--include-description",
    type=bool,
    is_flag=True,
    default=False,
    help="Include product description in output.",
)
@click.option(
    "--include-datasets",
    type=bool,
    is_flag=True,
    default=False,
    help="Include product dataset details in output.",
)
@click.option(
    "--include-keywords",
    type=bool,
    is_flag=True,
    default=False,
    help="Include product keyword details in output.",
)
@click.option(
    "--contains",
    "-c",
    type=str,
    multiple=True,
    help="Filter catalogue output. Returns products with attributes "
    "matching a string token.",
)
@click.option(
    "--overwrite-metadata-cache",
    cls=MutuallyExclusiveOption,
    type=bool,
    is_flag=True,
    default=False,
    help="Force to refresh the catalogue by overwriting the local cache.",
    mutually_exclusive=["no_metadata_cache"],
)
@click.option(
    "--no-metadata-cache",
    cls=MutuallyExclusiveOption,
    type=bool,
    is_flag=True,
    default=False,
    help="Bypass the use of cache.",
    mutually_exclusive=["overwrite_metadata_cache"],
)
@tqdm_disable_option
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "QUIET"]),
    default="INFO",
    help=(
        "Set the details printed to console by the command "
        "(based on standard logging library)."
    ),
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
    contains: list[str],
    overwrite_metadata_cache: bool,
    no_metadata_cache: bool,
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

    json_dump = describe_function(
        include_description=include_description,
        include_datasets=include_datasets,
        include_keywords=include_keywords,
        contains=contains,
        overwrite_metadata_cache=overwrite_metadata_cache,
        no_metadata_cache=no_metadata_cache,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )
    blank_logger.info(json_dump)
