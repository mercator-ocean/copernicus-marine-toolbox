import logging
import pathlib
from typing import Optional

import click

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.core_functions.login import login_function
from copernicusmarine.core_functions.utils import DEFAULT_CLIENT_BASE_DIRECTORY

logger = logging.getLogger("copernicus_marine_root_logger")


@click.group()
def cli_group_login() -> None:
    pass


@cli_group_login.command(
    "login",
    short_help="Create a configuration file with your Copernicus Marine credentials.",
    help="""
    Create a configuration file with your Copernicus Marine credentials.

    Create a configuration file under the $HOME/.copernicusmarine directory (overwritable with option --credentials-file).
    """,  # noqa
    epilog="""
    Examples:

    \b
    COPERNICUS_MARINE_SERVICE_USERNAME=<USERNAME> COPERNICUS_MARINE_SERVICE_PASSWORD=<PASSWORD> copernicusmarine login

    \b
    copernicusmarine login --username <USERNAME> --password <PASSWORD>

    \b
    copernicusmarine login
    > Username: [USER-INPUT]
    > Password: [USER-INPUT]
    """,  # noqa
)
@click.option(
    "--username",
    hide_input=False,
    help="If not set, search for environment variable"
    + " COPERNICUS_MARINE_SERVICE_USERNAME"
    + ", or else ask for user input.",
)
@click.option(
    "--password",
    hide_input=True,
    help="If not set, search for environment variable"
    + " COPERNICUS_MARINE_SERVICE_PASSWORD"
    + ", or else ask for user input.",
)
@click.option(
    "--configuration-file-directory",
    type=click.Path(path_type=pathlib.Path),
    default=DEFAULT_CLIENT_BASE_DIRECTORY,
    help="Path to the directory where the configuration file is stored.",
)
@click.option(
    "--overwrite-configuration-file",
    "-overwrite",
    is_flag=True,
    default=False,
    help="Flag to skip confirmation before overwriting configuration file.",
)
@click.option(
    "--skip-if-user-logged-in",
    is_flag=True,
    default=False,
    help="Flag to skip the logging process if the user is already logged in.",
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "QUIET"]),
    default="INFO",
    help=(
        "Set the details printed to console by the command "
        "(based on standard logging library)."
    ),
)
@log_exception_and_exit
def login(
    username: Optional[str],
    password: Optional[str],
    configuration_file_directory: pathlib.Path,
    overwrite_configuration_file: bool,
    skip_if_user_logged_in: bool,
    log_level: str = "INFO",
) -> None:
    if log_level == "QUIET":
        logger.disabled = True
        logger.setLevel(level="CRITICAL")
    else:
        logger.setLevel(level=log_level)
    login_function(
        username=username,
        password=password,
        configuration_file_directory=configuration_file_directory,
        overwrite_configuration_file=overwrite_configuration_file,
        skip_if_user_logged_in=skip_if_user_logged_in,
    )
