import logging
import pathlib
from typing import Optional, Union

import click

from copernicusmarine.command_line_interface.exception_handler import (
    log_exception_and_exit,
)
from copernicusmarine.command_line_interface.utils import (
    MutuallyExclusiveOption,
    assert_cli_args_are_not_set_except_create_template,
    credentials_file_option,
    force_dataset_part_option,
    force_dataset_version_option,
    force_download_option,
    tqdm_disable_option,
)
from copernicusmarine.core_functions import documentation_utils
from copernicusmarine.core_functions.click_custom_class import (
    CustomClickOptionsCommand,
)
from copernicusmarine.core_functions.fields_query_builder import (
    build_query,
    get_queryable_requested_fields,
)
from copernicusmarine.core_functions.get import (
    create_get_template,
    get_function,
)
from copernicusmarine.core_functions.models import ResponseGet

logger = logging.getLogger("copernicusmarine")
blank_logger = logging.getLogger("copernicusmarine_blank_logger")

DEFAULT_FIELDS_TO_INCLUDE = {
    "status",
    "message",
    "total_size",
    "number_of_files_to_download",
}


@click.group()
def cli_get() -> None:
    pass


@cli_get.command(
    "get",
    cls=CustomClickOptionsCommand,
    short_help="Download originally produced data files.",
    help=documentation_utils.GET["GET_DESCRIPTION_HELP"]
    + " See :ref:`describe <cli-describe>`. \n\nReturns\n "
    + documentation_utils.GET["GET_RESPONSE_HELP"],
    epilog="""
    Example to download all the files from a given dataset:

    .. code-block:: bash

        copernicusmarine get -i cmems_mod_nws_bgc-pft_myint_7km-3D-diato_P1M-m \n
    """,  # noqa
)
@click.option(
    "--dataset-id",
    "-i",
    type=str,
    default=None,
    help=documentation_utils.GET["DATASET_ID_HELP"],
)
@force_dataset_version_option
@force_dataset_part_option
@click.option(
    "--username",
    type=str,
    default=None,
    help=documentation_utils.GET["USERNAME_HELP"],
)
@click.option(
    "--password",
    type=str,
    default=None,
    help=documentation_utils.GET["PASSWORD_HELP"],
)
@click.option(
    "--no-directories",
    "-nd",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    help=documentation_utils.GET["NO_DIRECTORIES_HELP"],
    default=False,
    mutually_exclusive=["sync", "sync-delete", "skip-existing"],
)
@click.option(
    "--output-directory",
    "-o",
    type=click.Path(path_type=pathlib.Path),
    help=documentation_utils.GET["OUTPUT_DIRECTORY_HELP"],
)
@credentials_file_option
@click.option(
    "--overwrite",
    is_flag=True,
    default=False,
    cls=MutuallyExclusiveOption,
    help=documentation_utils.GET["OVERWRITE_HELP"],
    mutually_exclusive=["skip-existing", "sync", "sync-delete"],
)
@click.option(
    "--create-template",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.GET["CREATE_TEMPLATE_HELP"],
)
@click.option(
    "--request-file",
    type=click.Path(exists=True, path_type=pathlib.Path),
    help=documentation_utils.GET["REQUEST_FILE_HELP"],
)
@click.option(
    "--filter",
    "--filter-with-globbing-pattern",
    type=str,
    default=None,
    help=documentation_utils.GET["FILTER_HELP"],
)
@click.option(
    "--regex",
    "--filter-with-regular-expression",
    type=str,
    default=None,
    help=documentation_utils.GET["REGEX_HELP"],
)
@click.option(
    "--file-list",
    type=pathlib.Path,
    default=None,
    help=documentation_utils.GET["FILE_LIST_HELP"],
)
@click.option(
    "--create-file-list",
    type=str,
    default=None,
    help=documentation_utils.GET["CREATE_FILE_LIST_HELP"],
)
@click.option(
    "--sync",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    default=False,
    help=documentation_utils.GET["SYNC_HELP"],
    mutually_exclusive=["no-directories", "skip-existing"],
)
@click.option(
    "--sync-delete",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    default=False,
    help=documentation_utils.GET["SYNC_DELETE_HELP"],
    mutually_exclusive=["no-directories", "skip-existing"],
)
@click.option(
    "--skip-existing",
    cls=MutuallyExclusiveOption,
    is_flag=True,
    default=False,
    help=documentation_utils.GET["SKIP_EXISTING_HELP"],
    mutually_exclusive=["no-directories", "sync", "sync-delete"],
)
@click.option(
    "--index-parts",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.GET["INDEX_PARTS_HELP"],
)
@click.option(
    "--dry-run",
    type=bool,
    is_flag=True,
    default=False,
    help=documentation_utils.GET["DRY_RUN_HELP"],
)
@click.option(
    "--response-fields",
    "-r",
    type=str,
    default=None,
    help=documentation_utils.GET["RESPONSE_FIELDS_HELP"],
)
@click.option(
    "--max-concurrent-requests",
    type=int,
    default=15,
    help=documentation_utils.GET["MAX_CONCURRENT_REQUESTS_HELP"],
)
@tqdm_disable_option
@click.option(
    "--log-level",
    type=click.Choice(["DEBUG", "INFO", "WARN", "ERROR", "CRITICAL", "QUIET"]),
    default="INFO",
    help=documentation_utils.GET["LOG_LEVEL_HELP"],
)
@click.option(
    "--staging",
    type=bool,
    default=False,
    is_flag=True,
    hidden=True,
)
@force_download_option
@log_exception_and_exit
def get(
    dataset_id: Optional[str],
    dataset_version: Optional[str],
    dataset_part: Optional[str],
    username: Optional[str],
    password: Optional[str],
    no_directories: bool,
    output_directory: Optional[pathlib.Path],
    credentials_file: Optional[pathlib.Path],
    overwrite: bool,
    create_template: bool,
    request_file: Optional[pathlib.Path],
    filter: Optional[str],
    regex: Optional[str],
    file_list: Optional[pathlib.Path],
    create_file_list: Optional[str],
    sync: bool,
    sync_delete: bool,
    skip_existing: bool,
    index_parts: bool,
    dry_run: bool,
    response_fields: Optional[str],
    max_concurrent_requests: int,
    disable_progress_bar: bool,
    log_level: str,
    staging: bool,
    force_download: bool,
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

    response = get_function(
        dataset_id=dataset_id,
        force_dataset_version=dataset_version,
        force_dataset_part=dataset_part,
        username=username,
        password=password,
        no_directories=no_directories,
        output_directory=output_directory,
        credentials_file=credentials_file,
        overwrite=overwrite,
        request_file=request_file,
        filter_option=filter,
        regex=regex,
        file_list_path=file_list,
        create_file_list=create_file_list,
        sync=sync,
        sync_delete=sync_delete,
        skip_existing=skip_existing,
        index_parts=index_parts,
        dry_run=dry_run,
        max_concurrent_requests=max_concurrent_requests,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )

    if response_fields:
        fields_to_include = set(response_fields.replace(" ", "").split(","))
    elif dry_run:
        fields_to_include = {"all"}
    else:
        fields_to_include = DEFAULT_FIELDS_TO_INCLUDE
    included_fields: Optional[Union[set[str], dict]]
    if "all" in fields_to_include:
        included_fields = None
    elif "none" in fields_to_include:
        included_fields = set()
    else:
        queryable_fields = get_queryable_requested_fields(
            fields_to_include, ResponseGet, "--response-fields"
        )
        included_fields = build_query(set(queryable_fields), ResponseGet)

    blank_logger.info(
        response.model_dump_json(
            indent=2,
            include=included_fields,
            exclude_none=True,
            exclude_unset=True,
        )
    )
