import logging
import pathlib
from typing import Optional

from copernicusmarine.core_functions.credentials_utils import (
    copernicusmarine_configuration_file_exists,
    copernicusmarine_configuration_file_is_valid,
    credentials_file_builder,
)

logger = logging.getLogger("copernicus_marine_root_logger")


def login_function(
    username: Optional[str],
    password: Optional[str],
    configuration_file_directory: pathlib.Path,
    overwrite_configuration_file: bool,
    skip_if_user_logged_in: bool,
) -> bool:
    if (
        skip_if_user_logged_in
        and copernicusmarine_configuration_file_exists(
            configuration_file_directory
        )
        and copernicusmarine_configuration_file_is_valid(
            configuration_file_directory
        )
    ):
        logger.info("You are already logged in. Skipping login.")
        return True
    credentials_file = credentials_file_builder(
        username=username,
        password=password,
        configuration_file_directory=configuration_file_directory,
        overwrite_configuration_file=overwrite_configuration_file,
    )
    if credentials_file is not None:
        logger.info(f"Credentials file stored in {credentials_file}.")
        return True
    else:
        logger.info(
            "Invalid credentials. No configuration file have been modified."
        )
        logger.info(
            "Learn how to recover your credentials at: "
            "https://help.marine.copernicus.eu/en/articles/"
            "4444552-i-forgot-my-username-or-my-password-what-should-i-do"
        )
    return False
