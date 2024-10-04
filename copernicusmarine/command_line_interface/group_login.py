import logging
import pathlib
from typing import Optional

import click

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.core_functions import documentation_utils
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
    short_help="Create a configuration file with your Copernicus Marine credentials.",
    help=documentation_utils.LOGIN["LOGIN_DESCRIPTION_HELP"],  # noqa
    epilog="""
    .. code-block:: bash

        COPERNICUSMARINE_SERVICE_USERNAME=<USERNAME> COPERNICUSMARINE_SERVICE_PASSWORD=<PASSWORD> copernicusmarine login

    .. code-block:: bash

        copernicusmarine login --username <USERNAME> --password <PASSWORD>

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
    "--overwrite-configuration-file",
    "-overwrite",
    is_flag=True,
    default=False,
    help=documentation_utils.LOGIN["OVERWRITE_CONFIGURATION_FILE_HELP"],
)
@click.option(
    "--skip-if-user-logged-in",
    is_flag=True,
    default=False,
    help=documentation_utils.LOGIN["SKIP_IF_USER_LOGGED_IN_HELP"],
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
