import pathlib
from typing import Optional

from copernicusmarine.core_functions.login import login_function
from copernicusmarine.core_functions.utils import DEFAULT_CLIENT_BASE_DIRECTORY


def login(
    username: Optional[str] = None,
    password: Optional[str] = None,
    configuration_file_directory: pathlib.Path = DEFAULT_CLIENT_BASE_DIRECTORY,
    overwrite_configuration_file: bool = False,
    skip_if_user_logged_in: bool = False,
) -> bool:
    return login_function(
        username=username,
        password=password,
        configuration_file_directory=configuration_file_directory,
        overwrite_configuration_file=overwrite_configuration_file,
        skip_if_user_logged_in=skip_if_user_logged_in,
    )
