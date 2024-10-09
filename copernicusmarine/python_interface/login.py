import pathlib
from typing import Optional

from copernicusmarine.core_functions import decorators, documentation_utils
from copernicusmarine.core_functions.credentials_utils import (
    DEFAULT_CLIENT_BASE_DIRECTORY,
)
from copernicusmarine.core_functions.login import login_function


@decorators.docstring_parameter(documentation_utils.LOGIN)
def login(
    username: Optional[str] = None,
    password: Optional[str] = None,
    configuration_file_directory: pathlib.Path = DEFAULT_CLIENT_BASE_DIRECTORY,
    overwrite_configuration_file: bool = False,
    skip_if_user_logged_in: bool = False,
) -> bool:
    """
    {LOGIN_DESCRIPTION_HELP}

    Parameters
    ----------
    username : str, optional
        {USERNAME_HELP}
    password : str, optional
        {PASSWORD_HELP}
    configuration_file_directory : Union[pathlib.Path, str]
        {CONFIGURATION_FILE_DIRECTORY_HELP}
    overwrite_configuration_file : bool
        {OVERWRITE_CONFIGURATION_FILE_HELP}
    skip_if_user_logged_in : bool
        {SKIP_IF_USER_LOGGED_IN_HELP}
    """  # noqa
    return login_function(
        username=username,
        password=password,
        configuration_file_directory=configuration_file_directory,
        overwrite_configuration_file=overwrite_configuration_file,
        skip_if_user_logged_in=skip_if_user_logged_in,
    )
