import logging
import pathlib
from typing import Optional

import click

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.command_line_interface.utils import (
    MutuallyExclusiveOption,
    assert_cli_args_are_not_set_except_create_template,
    force_dataset_part_option,
    force_dataset_version_option,
    tqdm_disable_option,
)
from copernicusmarine.core_functions.deprecated import (
    DeprecatedClickOption,
    DeprecatedClickOptionsCommand,
)
from copernicusmarine.core_functions.get import (
    create_get_template,
    get_function,
)
from copernicusmarine.core_functions.services_utils import CommandType
from copernicusmarine.core_functions.utils import (
    OVERWRITE_LONG_OPTION,
    OVERWRITE_OPTION_HELP_TEXT,
    OVERWRITE_SHORT_OPTION,
)

logger = logging.getLogger("copernicus_marine_root_logger")


@click.group()
def cli_group_get() -> None:
    pass


@cli_group_get.command(
    "get",
    cls=DeprecatedClickOptionsCommand,
    short_help="Download originally produced data files.",
    help="""
    Download originally produced data files.

    Either one of --dataset-id or --dataset-url is required (can be found via the "describe" command).
    The function fetches the files recursively if a folder path is passed as URL.
    When provided a datasetID, all the files in the corresponding folder will be downloaded if none of the --filter or --regex options is specified.
    """,  # noqa
    epilog="""
    Examples:

    \b
    copernicusmarine get -nd -o data_folder --dataset-id cmems_mod_nws_bgc-pft_myint_7km-3D-diato_P1M-m
    """,  # noqa
)
@click.option(
    "--dataset-url",
    "-u",
    type=str,
    help="URL to the data files.",
)
@click.option(
    "--dataset-id",
    "-i",
    type=str,
    help="The datasetID.",
)
@force_dataset_version_option
@force_dataset_part_option
@click.option(
    "--username",
    type=str,
    default=None,
    help="If not set, search for environment variable"
    + " COPERNICUS_MARINE_SERVICE_USERNAME"
    + ", or else look for configuration files, or else ask for user input.",
)
@click.option(
    "--password",
    type=str,
    default=None,
    help="If not set, search for environment variable"
    + " COPERNICUS_MARINE_SERVICE_PASSWORD"
    + ", or else look for configuration files, or else ask for user input.",
)
@click.option(
    "--no-directories",
    "-nd",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help="Option to not recreate folder hierarchy in ouput directory.",
    default=False,
    mutually_exclusive=["sync"],
)
@click.option(
    "--show-outputnames",
    is_flag=True,
    help="Option to display the names of the"
    + " output files before download.",
    default=False,
)
@click.option(
    "--output-directory",
    "-o",
    type=click.Path(path_type=pathlib.Path),
    help="The destination directory for the downloaded files."
    + " Default is the current directory.",
)
@click.option(
    "--credentials-file",
    type=click.Path(path_type=pathlib.Path),
    help=(
        "Path to a credentials file if not in its default directory. "
        "Accepts .copernicusmarine-credentials / .netrc or _netrc / "
        "motuclient-python.ini files."
    ),
)
@click.option(
    "--force-download",
    is_flag=True,
    default=False,
    help="Flag to skip confirmation before download.",
)
@click.option(
    OVERWRITE_LONG_OPTION,
    OVERWRITE_SHORT_OPTION,
    is_flag=True,
    default=False,
    help=OVERWRITE_OPTION_HELP_TEXT,
)
@click.option(
    "--service",
    "--force-service",
    "-s",
    cls=DeprecatedClickOption,
    deprecated=["--force-service"],
    preferred="--service",
    type=str,
    help=(
        "Force download through one of the available services "
        f"using the service name among {CommandType.GET.service_names()} "
        f"or its short name among {CommandType.GET.service_short_names()}."
    ),
)
@click.option(
    "--create-template",
    type=bool,
    is_flag=True,
    default=False,
    help="Option to create a file get_template.json in your current directory "
    "containing CLI arguments. If specified, no other action will be performed.",
)
@click.option(
    "--request-file",
    type=click.Path(exists=True, path_type=pathlib.Path),
    help="Option to pass a file containing CLI arguments. "
    "The file MUST follow the structure of dataclass 'GetRequest'.",
)
@click.option(
    "--overwrite-metadata-cache",
    cls=MutuallyExclusiveOption,
    type=bool,
    is_flag=True,
    default=False,
    help="Force to refresh the catalogue by overwriting the local cache.",
    mutually_exclusive=["no_metadata_cache"],
)
@click.option(
    "--no-metadata-cache",
    cls=MutuallyExclusiveOption,
    type=bool,
    is_flag=True,
    default=False,
    help="Bypass the use of cache.",
    mutually_exclusive=["overwrite_metadata_cache"],
)
@click.option(
    "--filter",
    "--filter-with-globbing-pattern",
    type=str,
    default=None,
    help="A pattern that must match the absolute paths of "
    "the files to download.",
)
@click.option(
    "--regex",
    "--filter-with-regular-expression",
    type=str,
    default=None,
    help="The regular expression that must match the absolute paths of "
    "the files to download.",
)
@click.option(
    "--file-list",
    type=pathlib.Path,
    default=None,
    help="A path to a text file that list filenames line by line. "
    "Filenames must match the absolute paths of "
    "the files to download.",
)
@click.option(
    "--create-file-list",
    type=str,
    default=None,
    help="Option to only create a file containing "
    "the names of the targeted files instead of downloading them. "
    "It writes the file in the directory specified with the "
    "--output-directory option (default to current directory). "
    "The file name specified should end with '.txt' or '.csv' "
    "If specified, no other action will be performed. "
    "Please find more information in the README.",
)
@click.option(
    "--download-file-list",
    type=bool,
    is_flag=True,
    cls=DeprecatedClickOption,
    deprecated=["--download-file-list"],
    preferred="--create-file-list",
)
@click.option(
    "--sync",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    default=False,
    help="Option to synchronize the local directory with "
    "the remote directory. See the documentation for more details.",
    mutually_exclusive=["no-directories"],
)
@click.option(
    "--sync-delete",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    default=False,
    help="Option to delete local files that are not present on "
    "the remote server while applying sync.",
    mutually_exclusive=["no-directories"],
)
@click.option(
    "--index-parts",
    type=bool,
    is_flag=True,
    default=False,
    help="Option to get the index files of an INSITU dataset. Temporary option.",
)
@tqdm_disable_option
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "QUIET"]),
    default="INFO",
    help=(
        "Set the details printed to console by the command "
        "(based on standard logging library)."
    ),
)
@click.option(
    "--staging",
    type=bool,
    default=False,
    is_flag=True,
    hidden=True,
)
@log_exception_and_exit
def get(
    dataset_url: Optional[str],
    dataset_id: Optional[str],
    dataset_version: Optional[str],
    dataset_part: Optional[str],
    username: Optional[str],
    password: Optional[str],
    no_directories: bool,
    show_outputnames: bool,
    output_directory: Optional[pathlib.Path],
    credentials_file: Optional[pathlib.Path],
    force_download: bool,
    overwrite_output_data: bool,
    create_template: bool,
    request_file: Optional[pathlib.Path],
    service: Optional[str],
    overwrite_metadata_cache: bool,
    no_metadata_cache: bool,
    filter: Optional[str],
    regex: Optional[str],
    file_list: Optional[pathlib.Path],
    create_file_list: Optional[str],
    download_file_list: bool,
    sync: bool,
    sync_delete: bool,
    index_parts: bool,
    disable_progress_bar: bool,
    log_level: str,
    staging: bool,
):
    if log_level == "QUIET":
        logger.disabled = True
        logger.setLevel(level="CRITICAL")
    else:
        logger.setLevel(level=log_level)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("DEBUG mode activated")

    if create_template:
        assert_cli_args_are_not_set_except_create_template(
            click.get_current_context()
        )
        create_get_template()
        return

    return get_function(
        dataset_url=dataset_url,
        dataset_id=dataset_id,
        force_dataset_version=dataset_version,
        force_dataset_part=dataset_part,
        username=username,
        password=password,
        no_directories=no_directories,
        show_outputnames=show_outputnames,
        output_directory=output_directory,
        credentials_file=credentials_file,
        force_download=force_download,
        overwrite_output_data=overwrite_output_data,
        request_file=request_file,
        force_service=service,
        overwrite_metadata_cache=overwrite_metadata_cache,
        no_metadata_cache=no_metadata_cache,
        filter=filter,
        regex=regex,
        file_list_path=file_list,
        create_file_list=create_file_list,
        download_file_list=download_file_list,
        sync=sync,
        sync_delete=sync_delete,
        index_parts=index_parts,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )
