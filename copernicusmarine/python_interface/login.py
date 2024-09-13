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

    Parameters
    ----------
    username : str, optional
        If not set, searches for the environment variable `COPERNICUSMARINE_SERVICE_USERNAME`,
        or else asks for user input.
    password : str, optional
        If not set, searches for the environment variable `COPERNICUSMARINE_SERVICE_PASSWORD`,
        or else asks for user input.
    configuration_file_directory : Union[pathlib.Path, str]
        Path to the directory where the configuration file is stored.
    overwrite_configuration_file : bool
        Flag to skip confirmation before overwriting the configuration file.
    skip_if_user_logged_in : bool
        Flag to skip the logging process if the user is already logged in.
    """  # noqa
    return login_function(
        username=username,
        password=password,
        configuration_file_directory=configuration_file_directory,
        overwrite_configuration_file=overwrite_configuration_file,
        skip_if_user_logged_in=skip_if_user_logged_in,
    )
