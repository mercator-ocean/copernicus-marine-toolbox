import logging
import pathlib
from typing import Optional

import click

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.core_functions import documentation_utils
from copernicusmarine.core_functions.click_custom_class import (
    CustomClickOptionsCommand,
)
from copernicusmarine.core_functions.credentials_utils import (
    DEFAULT_CLIENT_BASE_DIRECTORY,
)
from copernicusmarine.core_functions.login import login_function

logger = logging.getLogger("copernicusmarine")


@click.group()
def cli_login() -> None:
    pass


@cli_login.command(
    "login",
    cls=CustomClickOptionsCommand,
    short_help="Create a configuration file with your Copernicus Marine credentials.",
    help=documentation_utils.LOGIN["LOGIN_DESCRIPTION_HELP"]
    + " \n\nReturns\n "
    + documentation_utils.LOGIN["LOGIN_RESPONSE_HELP"],  # noqa
    epilog="""
    Examples:

    Using environment variables:

    .. code-block:: bash

        COPERNICUSMARINE_SERVICE_USERNAME=<USERNAME> COPERNICUSMARINE_SERVICE_PASSWORD=<PASSWORD> copernicusmarine login

    Using command line arguments:

    .. code-block:: bash

        copernicusmarine login --username <USERNAME> --password <PASSWORD>

    Using directly user input:

    .. code-block:: bash

        copernicusmarine login
        > Username: [USER-INPUT]
        > Password: [USER-INPUT]
    """,  # noqa
)
@click.option(
    "--username",
    hide_input=False,
    help=documentation_utils.LOGIN["USERNAME_HELP"],
)
@click.option(
    "--password",
    hide_input=True,
    help=documentation_utils.LOGIN["PASSWORD_HELP"],
)
@click.option(
    "--configuration-file-directory",
    type=click.Path(path_type=pathlib.Path),
    default=DEFAULT_CLIENT_BASE_DIRECTORY,
    help=documentation_utils.LOGIN["CONFIGURATION_FILE_DIRECTORY_HELP"],
)
@click.option(
    "--force-overwrite",
    is_flag=True,
    default=False,
    help=documentation_utils.LOGIN["FORCE_OVERWRITE_HELP"],
)
@click.option(
    "--check-credentials-valid",
    is_flag=True,
    default=False,
    help=documentation_utils.LOGIN["CHECK_CREDENTIALS_VALID_HELP"],
)
@click.option(
    "--credentials-file",
    type=click.Path(path_type=pathlib.Path),
    help=documentation_utils.LOGIN["CREDENTIALS_FILE_HELP"],
)
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "QUIET"]),
    default="INFO",
    help=documentation_utils.LOGIN["LOG_LEVEL_HELP"],
)
@log_exception_and_exit
def login(
    username: Optional[str],
    password: Optional[str],
    configuration_file_directory: pathlib.Path,
    force_overwrite: bool,
    check_credentials_valid: bool,
    credentials_file: Optional[pathlib.Path],
    log_level: str = "INFO",
) -> None:
    if log_level == "QUIET":
        logger.disabled = True
        logger.setLevel(level="CRITICAL")
    else:
        logger.setLevel(level=log_level)
    if not login_function(
        username=username,
        password=password,
        configuration_file_directory=configuration_file_directory,
        force_overwrite=force_overwrite,
        check_credentials_valid=check_credentials_valid,
        configuration_file=credentials_file,
    ):
        exit(1)
