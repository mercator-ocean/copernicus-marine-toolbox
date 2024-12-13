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
    force_overwrite: bool = False,
    check_credentials_valid: bool = False,
    credentials_file: Optional[pathlib.Path] = None,
) -> bool:
    """
    Create a configuration file with your Copernicus Marine credentials under the ``$HOME/.copernicusmarine`` directory (overwritable with the ``force_overwrite`` option).

    Parameters
    ----------
    username : str, optional
        If not set, search for environment variable COPERNICUSMARINE_SERVICE_USERNAME, else ask for user input.
    password : str, optional
        If not set, search for environment variable COPERNICUSMARINE_SERVICE_PASSWORD, else ask for user input.
    configuration_file_directory : Union[pathlib.Path, str]
        Path to the directory where the configuration file will be stored.
    force_overwrite : bool
        Flag to skip confirmation before overwriting configuration file.
    check_credentials_valid : bool
        Flag to check if the credentials are valid. No other action will be performed. The validity will be check in this order:
        1. Check if the credentials are valid with the provided username and password.
        2. Check if the credentials are valid in the environment variables.
        3. Check if the credentials are valid in the configuration file.
        When any is found (valid or not valid), will return immediately.
    credentials_file : Union[pathlib.Path, str], optional
        Path to a credentials file if not in its default directory (``$HOME/.copernicusmarine``). Accepts .copernicusmarine-credentials / .netrc or _netrc / motuclient-python.ini files. Will only be taken into account when checking the credentials validity.

    Returns
    -------
    bool
        True value if the login was successfully completed, False otherwise.
    """  # noqa
    return login_function(
        username=username,
        password=password,
        configuration_file_directory=configuration_file_directory,
        force_overwrite=force_overwrite,
        check_credentials_valid=check_credentials_valid,
        configuration_file=credentials_file,
    )
