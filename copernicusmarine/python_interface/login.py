import pathlib
from typing import Optional

from copernicusmarine.core_functions.credentials_utils import (
    DEFAULT_CLIENT_BASE_DIRECTORY,
)
from copernicusmarine.core_functions.login import login_function


def login(
    username: Optional[str] = None,
    password: Optional[str] = None,
    configuration_file_directory: pathlib.Path = DEFAULT_CLIENT_BASE_DIRECTORY,
    overwrite_configuration_file: bool = False,
    skip_if_user_logged_in: bool = False,
) -> bool:
    """
    Create a configuration file with your Copernicus Marine credentials.

    :param username: If not set, search for environment variable COPERNICUSMARINE_SERVICE_USERNAME, or else ask for user input.
    :type username: str, optional
    :param password: If not set, search for environment variable COPERNICUSMARINE_SERVICE_PASSWORD, or else ask for user input.
    :type password: str, optional
    :param configuration_file_directory: Path to the directory where the configuration file is stored.
    :type configuration_file_directory: Union[pathlib.Path, str]
    :param overwrite_configuration_file: Flag to skip confirmation before overwriting configuration file.
    :type overwrite_configuration_file: bool
    :param skip_if_user_logged_in: Flag to skip the logging process if the user is already logged in.
    :type skip_if_user_logged_in: bool
    """  # noqa
    return login_function(
        username=username,
        password=password,
        configuration_file_directory=configuration_file_directory,
        overwrite_configuration_file=overwrite_configuration_file,
        skip_if_user_logged_in=skip_if_user_logged_in,
    )
