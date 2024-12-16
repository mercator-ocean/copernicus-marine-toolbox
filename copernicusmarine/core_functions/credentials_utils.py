import base64
import configparser
import logging
import pathlib
from netrc import netrc
from platform import system
from typing import Literal, Optional

import click
import lxml.html
import requests

from copernicusmarine.core_functions.environment_variables import (
    COPERNICUSMARINE_CREDENTIALS_DIRECTORY,
    COPERNICUSMARINE_SERVICE_PASSWORD,
    COPERNICUSMARINE_SERVICE_USERNAME,
)
from copernicusmarine.core_functions.sessions import (
    BearerAuth,
    get_configured_requests_session,
)

logger = logging.getLogger("copernicusmarine")

USER_DEFINED_CACHE_DIRECTORY: str = (
    COPERNICUSMARINE_CREDENTIALS_DIRECTORY or ""
)
DEFAULT_CLIENT_BASE_DIRECTORY: pathlib.Path = (
    pathlib.Path(USER_DEFINED_CACHE_DIRECTORY)
    if USER_DEFINED_CACHE_DIRECTORY
    else pathlib.Path.home()
) / ".copernicusmarine"
DEFAULT_CLIENT_CREDENTIALS_FILENAME = ".copernicusmarine-credentials"
DEFAULT_CLIENT_CREDENTIALS_FILEPATH = (
    DEFAULT_CLIENT_BASE_DIRECTORY / DEFAULT_CLIENT_CREDENTIALS_FILENAME
)
RECOVER_YOUR_CREDENTIALS_MESSAGE = (
    "Learn how to recover your credentials at: "
    "https://help.marine.copernicus.eu/en/articles/"
    "4444552-i-forgot-my-username-or-my-password-what-should-i-do"
)

COPERNICUS_MARINE_AUTH_SYSTEM_DOMAIN = "auth.marine.copernicus.eu"
COPERNICUS_MARINE_AUTH_SYSTEM_URL = (
    f"https://{COPERNICUS_MARINE_AUTH_SYSTEM_DOMAIN}/"
)
COPERNICUS_MARINE_AUTH_SYSTEM_TOKEN_ENDPOINT = (
    COPERNICUS_MARINE_AUTH_SYSTEM_URL
    + "realms/MIS/protocol/openid-connect/token"
)
COPERNICUS_MARINE_AUTH_SYSTEM_USERINFO_ENDPOINT = (
    COPERNICUS_MARINE_AUTH_SYSTEM_URL
    + "realms/MIS/protocol/openid-connect/userinfo"
)

COPERNICUS_MARINE_MARINE_AUTH_OLD_SYSTEM_DOMAIN = "cmems-cas.cls.fr"

COPERNICUS_MARINE_MARINE_AUTH_OLD_SYSTEM_URL = (
    f"https://{COPERNICUS_MARINE_MARINE_AUTH_OLD_SYSTEM_DOMAIN}/cas/login"
)
ACCEPTED_HOSTS_NETRC_FILE = [
    "nrt.cmems-du.eu",
    "my.cmems-du.eu",
    COPERNICUS_MARINE_AUTH_SYSTEM_DOMAIN,
    "default_host",
]
DEPRECATED_HOSTS = [
    "nrt.cmems-du.eu",
    "my.cmems-du.eu",
]


class CredentialsCannotBeNone(Exception):
    """
    Exception raised when credentials are not set.

    To use the Copernicus Marine Service, you need to provide a username and
    a password. You can set them as environment variables or pass them as
    arguments to the function or use the :func:`~copernicusmarine.login` command.
    To register and create your valid credentials, please visit the
    Copernicus Marine `registration page <https://data.marine.copernicus.eu/register>`_
    """

    pass


class InvalidUsernameOrPassword(Exception):
    """
    Exception raised when the username or password are invalid.

    To register and create your valid credentials, please visit:
    Copernicus Marine `registration page <https://data.marine.copernicus.eu/register>`_
    """

    pass


class CouldNotConnectToAuthenticationSystem(Exception):
    """
    Exception raised when the client could not connect to the authentication system.

    Please check the following common problems:

    - Check your internet connection
    - Make sure to authorize ``cmems-cas.cls.fr`` and/or ``auth.marine.copernicus.eu`` domains

    If none of this worked, maybe the authentication system is down, please try again later.
    """  # noqa

    pass


def _warning_netrc_deprecated_hosts():
    logger.warning(
        "The following hosts are deprecated and will be removed in future versions: "
        f"{DEPRECATED_HOSTS}. "
        "Please update your netrc file to use the new authentication system domain: "
        f"{COPERNICUS_MARINE_AUTH_SYSTEM_DOMAIN}."
    )


def _warning_motuclient_deprecated():
    logger.warning(
        "The motuclient configuration file is deprecated "
        "and will be removed in future versions. Please use "
        "the login command and or function to create a new configuration file."
    )


def _load_credential_from_copernicus_marine_configuration_file(
    credential_type: Literal["username", "password"],
    configuration_filename: pathlib.Path,
) -> Optional[str]:
    configuration_file = open(configuration_filename)
    configuration_string = base64.standard_b64decode(
        configuration_file.read()
    ).decode("utf8")
    config = configparser.RawConfigParser()
    config.read_string(configuration_string)
    credential = config.get("credentials", credential_type)
    if credential:
        logger.debug(f"{credential_type} loaded from {configuration_filename}")
    return credential


def _load_credential_from_netrc_configuration_file(
    credential_type: Literal["username", "password"],
    configuration_filename: pathlib.Path,
) -> Optional[str]:
    authenticator = None
    for host in ACCEPTED_HOSTS_NETRC_FILE:
        authenticator = netrc(configuration_filename).authenticators(host=host)
        if (
            authenticator
            and host in DEPRECATED_HOSTS
            and credential_type == "username"
        ):
            _warning_netrc_deprecated_hosts()
        if authenticator:
            break
    if authenticator:
        username, _, password = authenticator
        logger.debug(f"{credential_type} loaded from {configuration_filename}")
        return username if credential_type == "username" else password
    else:
        return None


def _load_credential_from_motu_configuration_file(
    credential_type: Literal["username", "password"],
    configuration_filename: pathlib.Path,
) -> Optional[str]:
    motu_file = open(configuration_filename)
    motu_credential_type = "user" if credential_type == "username" else "pwd"
    config = configparser.RawConfigParser()
    config.read_string(motu_file.read())
    credential = config.get("Main", motu_credential_type)
    if credential:
        logger.debug(f"{credential_type} loaded from {configuration_filename}")
    return credential


def _retrieve_credential_from_prompt(
    credential_type: Literal["username", "password"], hide_input: bool
) -> str:
    if credential_type == "username":
        logger.info(
            "Downloading Copernicus Marine data requires a Copernicus Marine username "
            "and password, sign up for free at:"
            " https://data.marine.copernicus.eu/register"
        )
    return click.prompt(
        "Copernicus Marine " + credential_type, hide_input=hide_input
    )


def _retrieve_credential_from_environment_variable(
    credential_type: Literal["username", "password"]
) -> Optional[str]:
    if credential_type == "username":
        logger.debug("Tried to load username from environment variable")
        return COPERNICUSMARINE_SERVICE_USERNAME
    if credential_type == "password":
        logger.debug("Tried to load password from environment variable")
        return COPERNICUSMARINE_SERVICE_PASSWORD


def _retrieve_credential_from_custom_configuration_files(
    credential_type: Literal["username", "password"],
    credentials_file: pathlib.Path,
) -> Optional[str]:
    if "netrc" in str(credentials_file):
        credential = _load_credential_from_netrc_configuration_file(
            credential_type, credentials_file
        )
    elif "motuclient" in str(credentials_file):
        if credential_type == "username":
            _warning_motuclient_deprecated()
        credential = _load_credential_from_motu_configuration_file(
            credential_type, credentials_file
        )
    else:
        credential = (
            _load_credential_from_copernicus_marine_configuration_file(
                credential_type, credentials_file
            )
        )
    return credential


def _retrieve_credential_from_default_configuration_files(
    credential_type: Literal["username", "password"],
) -> Optional[str]:
    copernicus_marine_configuration_file = pathlib.Path(
        DEFAULT_CLIENT_CREDENTIALS_FILEPATH
    )
    motu_configuration_file = pathlib.Path(
        pathlib.Path.home() / "motuclient" / "motuclient-python.ini"
    )
    netrc_configuration_file = pathlib.Path(
        pathlib.Path.home() / ("_netrc" if system() == "Windows" else ".netrc")
    )
    if copernicus_marine_configuration_file.exists():
        credential = (
            _load_credential_from_copernicus_marine_configuration_file(
                credential_type,
                copernicus_marine_configuration_file,
            )
        )
    elif motu_configuration_file.exists():
        if credential_type == "username":
            _warning_motuclient_deprecated()
        credential = _load_credential_from_motu_configuration_file(
            credential_type, motu_configuration_file
        )
    elif netrc_configuration_file.exists():
        credential = _load_credential_from_netrc_configuration_file(
            credential_type, netrc_configuration_file
        )
    else:
        credential = None
    return credential


def _retrieve_credential_from_configuration_files(
    credential_type: Literal["username", "password"],
    credentials_file: Optional[pathlib.Path],
) -> Optional[str]:
    if credentials_file and credentials_file.exists():
        credential = _retrieve_credential_from_custom_configuration_files(
            credential_type, credentials_file
        )
    else:
        credential = _retrieve_credential_from_default_configuration_files(
            credential_type
        )
    return credential


def copernicusmarine_configuration_file_exists(
    configuration_file_directory: pathlib.Path,
) -> bool:
    configuration_filename = pathlib.Path(
        configuration_file_directory / DEFAULT_CLIENT_CREDENTIALS_FILENAME
    )
    return configuration_filename.exists()


def copernicusmarine_credentials_are_valid(
    configuration_file: Optional[pathlib.Path],
    username: Optional[str],
    password: Optional[str],
):
    if username and password:
        if _are_copernicus_marine_credentials_valid(username, password):
            logger.info("Valid credentials from input username and password.")
            return True
        else:
            logger.error(
                "Invalid credentials from input username and password."
            )
            logger.info(RECOVER_YOUR_CREDENTIALS_MESSAGE)
            return False
    elif (
        COPERNICUSMARINE_SERVICE_USERNAME and COPERNICUSMARINE_SERVICE_PASSWORD
    ):
        if _are_copernicus_marine_credentials_valid(
            COPERNICUSMARINE_SERVICE_USERNAME,
            COPERNICUSMARINE_SERVICE_PASSWORD,
        ):
            logger.info(
                "Valid credentials from environment variables: "
                "COPERNICUSMARINE_SERVICE_USERNAME and "
                "COPERNICUSMARINE_SERVICE_PASSWORD."
            )
            return True
        else:
            logger.error(
                "Invalid credentials from environment variables: "
                "COPERNICUSMARINE_SERVICE_USERNAME and "
                "COPERNICUSMARINE_SERVICE_PASSWORD."
            )
            logger.info(RECOVER_YOUR_CREDENTIALS_MESSAGE)
            return False
    elif (
        username := _retrieve_credential_from_configuration_files(
            "username", configuration_file
        )
    ) and (
        password := _retrieve_credential_from_configuration_files(
            "password", configuration_file
        )
    ):
        if _are_copernicus_marine_credentials_valid(username, password):
            logger.info("Valid credentials from configuration file.")
            return True
        else:
            logger.error("Invalid credentials from configuration file.")
            logger.info(RECOVER_YOUR_CREDENTIALS_MESSAGE)
    elif configuration_file:
        logger.info(
            f"No credentials found in configuration file {configuration_file}."
        )
        logger.info(
            "Please be sure the configuration file is correct: "
            "it exists and the format is correct (especially in "
            "the case of netrc or motuclient file)."
        )
        return False
    else:
        logger.info("No credentials found.")
        logger.info(
            "Please provide credentials as arguments or environment "
            "variables, or use the 'login' command to create a credentials file."
        )
    return False


def create_copernicusmarine_configuration_file(
    username: str,
    password: str,
    configuration_file_directory: pathlib.Path,
    force_overwrite: bool,
) -> tuple[Optional[pathlib.Path], bool]:
    configuration_lines = [
        "[credentials]\n",
        f"username={username}\n",
        f"password={password}\n",
    ]
    configuration_filename = pathlib.Path(
        configuration_file_directory / DEFAULT_CLIENT_CREDENTIALS_FILENAME
    )
    if configuration_filename.exists() and not force_overwrite:
        confirmed = click.confirm(
            f"File {configuration_filename} already exists, overwrite it ?"
        )
        if not confirmed:
            logger.error("Abort")
            return None, True

    configuration_file_directory.mkdir(parents=True, exist_ok=True)
    configuration_file = open(configuration_filename, "w")
    configuration_string = base64.b64encode(
        "".join(configuration_lines).encode("ascii", "strict")
    ).decode("utf8")
    configuration_file.write(configuration_string)
    configuration_file.close()
    return configuration_filename, False


def _check_credentials_with_old_cas(username: str, password: str) -> bool:
    logger.debug("Checking user credentials...")
    service = "copernicus-marine-client"
    cmems_cas_login_url = (
        f"{COPERNICUS_MARINE_MARINE_AUTH_OLD_SYSTEM_URL}?service={service}"
    )
    conn_session = get_configured_requests_session()
    logger.debug(f"GETing {cmems_cas_login_url}...")
    login_session = conn_session.get(
        cmems_cas_login_url, proxies=conn_session.proxies
    )
    login_session.raise_for_status()
    login_from_html = lxml.html.fromstring(login_session.text)
    hidden_elements_from_html = login_from_html.xpath(
        '//form//input[@type="hidden"]'
    )
    playload = {
        he.attrib["name"]: he.attrib["value"]
        for he in hidden_elements_from_html
    }
    playload["username"] = username
    playload["password"] = password
    logger.debug(f"POSTing credentials to {cmems_cas_login_url}...")
    login_response = conn_session.post(
        cmems_cas_login_url, data=playload, proxies=conn_session.proxies
    )
    login_response.raise_for_status()
    login_success = 'class="success"' in login_response.text
    logger.debug("User credentials checked")
    return login_success


def _check_credentials_with_cas(username: str, password: str) -> bool:
    keycloak_url = COPERNICUS_MARINE_AUTH_SYSTEM_TOKEN_ENDPOINT
    client_id = "toolbox"
    scope = "openid profile email"

    data = {
        "client_id": client_id,
        "grant_type": "password",
        "username": username,
        "password": password,
        "scope": scope,
    }
    conn_session = get_configured_requests_session()
    logger.debug(f"POSTing credentials to {keycloak_url}...")
    response = conn_session.post(
        keycloak_url, data=data, proxies=conn_session.proxies
    )
    response.raise_for_status()
    if response.status_code == 200:
        token_response = response.json()
        access_token = token_response["access_token"]
        bearer_auth = BearerAuth(access_token)
        userinfo_url = COPERNICUS_MARINE_AUTH_SYSTEM_USERINFO_ENDPOINT
        logger.debug(f"GETing {userinfo_url}...")
        response = conn_session.get(
            userinfo_url, auth=bearer_auth, proxies=conn_session.proxies
        )
        response.raise_for_status()
        if response.status_code == 200:
            return True
        else:
            return False
    else:
        return False


def _are_copernicus_marine_credentials_valid_old_system(
    username: str, password: str
) -> bool:
    number_of_retry = 3
    user_is_active = None
    while (user_is_active not in [True, False]) and number_of_retry > 0:
        try:
            user_is_active = _check_credentials_with_old_cas(
                username=username, password=password
            )
        except requests.exceptions.ConnectTimeout:
            number_of_retry -= 1
        except requests.exceptions.ConnectionError:
            number_of_retry -= 1
    if user_is_active is None:
        raise CouldNotConnectToAuthenticationSystem()
    return user_is_active


def _are_copernicus_marine_credentials_valid(
    username: str, password: str
) -> bool:
    try:
        result = _are_copernicus_marine_credentials_valid_new_system(
            username, password
        )
        return result

    except Exception as e:
        logger.debug(
            f"Could not connect with new authentication system because of: {e}"
        )
        logger.debug("Trying with old authentication system...")
        return _are_copernicus_marine_credentials_valid_old_system(
            username, password
        )


def _are_copernicus_marine_credentials_valid_new_system(
    username: str, password: str
) -> bool:
    number_of_retry = 3
    user_is_active = None
    while (user_is_active not in [True, False]) and number_of_retry > 0:
        try:
            user_is_active = _check_credentials_with_cas(
                username=username, password=password
            )
        except requests.exceptions.ConnectTimeout:
            number_of_retry -= 1
        except requests.exceptions.ConnectionError:
            number_of_retry -= 1
    if user_is_active is None:
        raise CouldNotConnectToAuthenticationSystem()
    return user_is_active


def get_credential(
    credential: Optional[str],
    credential_type: Literal["username", "password"],
    hide_input: bool,
    credentials_file: Optional[pathlib.Path],
) -> str:
    if not credential:
        credential = _retrieve_credential_from_environment_variable(
            credential_type
        )
        if not credential:
            credential = _retrieve_credential_from_configuration_files(
                credential_type=credential_type,
                credentials_file=credentials_file,
            )
            if not credential:
                credential = _retrieve_credential_from_prompt(
                    credential_type, hide_input=hide_input
                )
                if not credential:
                    raise ValueError(f"{credential} cannot be None")
    else:
        logger.debug("Credentials loaded from function arguments")
    return credential


def get_and_check_username_password(
    username: Optional[str],
    password: Optional[str],
    credentials_file: Optional[pathlib.Path],
) -> tuple[str, str]:
    username, password = get_username_password(
        username=username, password=password, credentials_file=credentials_file
    )
    copernicus_marine_credentials_are_valid = (
        _are_copernicus_marine_credentials_valid(
            username,
            password,
        )
    )
    if not copernicus_marine_credentials_are_valid:
        raise InvalidUsernameOrPassword(
            "Learn how to recover your credentials at: "
            "https://help.marine.copernicus.eu/en/articles/"
            "4444552-i-forgot-my-username-or-my-password-what-should-i-do"
        )
    return (username, password)


def get_username_password(
    username: Optional[str],
    password: Optional[str],
    credentials_file: Optional[pathlib.Path],
) -> tuple[str, str]:
    username = get_credential(
        username,
        "username",
        hide_input=False,
        credentials_file=credentials_file,
    )
    password = get_credential(
        password,
        "password",
        hide_input=True,
        credentials_file=credentials_file,
    )
    return (username, password)


def _get_credential_from_environment_variable_or_prompt(
    credential: Optional[str],
    credential_type: Literal["username", "password"],
    hide_input: bool,
) -> str:
    if not credential:
        credential = _retrieve_credential_from_environment_variable(
            credential_type
        )
        if not credential:
            credential = _retrieve_credential_from_prompt(
                credential_type, hide_input
            )
            if not credential:
                raise CredentialsCannotBeNone(credential_type)
    return credential


def credentials_file_builder(
    username: Optional[str],
    password: Optional[str],
    configuration_file_directory: pathlib.Path,
    force_overwrite: bool,
) -> tuple[Optional[pathlib.Path], bool]:
    """
    Returns:
        a path to the configuration file: if none, the credentials are not valid

        a boolean that indicates
        if the user aborted the creation of the configuration file
    """
    username = _get_credential_from_environment_variable_or_prompt(
        username, "username", False
    )
    password = _get_credential_from_environment_variable_or_prompt(
        password, "password", True
    )
    copernicus_marine_credentials_are_valid = (
        _are_copernicus_marine_credentials_valid(username, password)
    )
    if copernicus_marine_credentials_are_valid:
        (
            configuration_file,
            has_been_aborted,
        ) = create_copernicusmarine_configuration_file(
            username=username,
            password=password,
            configuration_file_directory=configuration_file_directory,
            force_overwrite=force_overwrite,
        )
        if has_been_aborted:
            return configuration_file, has_been_aborted
        if configuration_file:
            return configuration_file, False
    return None, False
