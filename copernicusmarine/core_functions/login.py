import logging
import pathlib
from typing import Optional

from copernicusmarine.core_functions.credentials_utils import (
    RECOVER_YOUR_CREDENTIALS_MESSAGE,
    copernicusmarine_credentials_are_valid,
    credentials_file_builder,
)

logger = logging.getLogger("copernicusmarine")


def login_function(
    username: Optional[str],
    password: Optional[str],
    configuration_file_directory: pathlib.Path,
    force_overwrite: bool,
    check_credentials_valid: bool,
) -> bool:
    if check_credentials_valid:
        logger.info("Checking if credentials are valid.")
        if copernicusmarine_credentials_are_valid(
            configuration_file_directory, username, password
        ):
            return True
        else:
            return False
    credentials_file = credentials_file_builder(
        username=username,
        password=password,
        configuration_file_directory=configuration_file_directory,
        force_overwrite=force_overwrite,
    )
    if credentials_file is not None:
        logger.info(f"Credentials file stored in {credentials_file}.")
        return True
    else:
        logger.info(
            "Invalid credentials. No configuration file have been modified."
        )
        logger.info(RECOVER_YOUR_CREDENTIALS_MESSAGE)
    return False
