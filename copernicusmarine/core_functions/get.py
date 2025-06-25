import json
import logging
import os
import pathlib
from typing import Optional

from copernicusmarine.core_functions.credentials_utils import (
    get_and_check_username_password,
)
from copernicusmarine.core_functions.marine_datastore_config import (
    MarineDataStoreConfig,
    get_config_and_check_version_get,
)
from copernicusmarine.core_functions.models import CommandType, ResponseGet
from copernicusmarine.core_functions.request_structure import (
    GetRequest,
    filter_to_regex,
    overload_regex_with_additionnal_filter,
)
from copernicusmarine.core_functions.services_utils import (
    RetrievalService,
    get_retrieval_service,
)
from copernicusmarine.core_functions.utils import get_unique_filepath
from copernicusmarine.download_functions.download_original_files import (
    download_original_files,
)

logger = logging.getLogger("copernicusmarine")


def get_function(
    dataset_id: Optional[str],
    force_dataset_version: Optional[str],
    force_dataset_part: Optional[str],
    username: Optional[str],
    password: Optional[str],
    no_directories: bool,
    output_directory: Optional[pathlib.Path],
    credentials_file: Optional[pathlib.Path],
    overwrite: bool,
    request_file: Optional[pathlib.Path],
    filter_option: Optional[str],
    regex: Optional[str],
    file_list_path: Optional[pathlib.Path],
    create_file_list: Optional[str],
    sync: bool,
    sync_delete: bool,
    index_parts: bool,
    skip_existing: bool,
    dry_run: bool,
    max_concurrent_requests: int,
    disable_progress_bar: bool,
    staging: bool,
) -> ResponseGet:
    marine_datastore_config = get_config_and_check_version_get(staging)
    if staging:
        logger.warning(
            "Detecting staging flag for get command. "
            "Data will come from the staging environment."
        )

    get_request = GetRequest(dataset_id=dataset_id or "")
    if request_file:
        get_request.from_file(request_file)
    if not get_request.dataset_id:
        raise ValueError("Please provide a dataset id for a get request.")
    request_update_dict = {
        "force_dataset_version": force_dataset_version,
        "output_directory": output_directory,
    }
    get_request.update(request_update_dict)

    # Specific treatment for default values:
    # In order to not overload arguments with default values
    # TODO is this really useful?
    if force_dataset_version:
        get_request.force_dataset_version = force_dataset_version
    if force_dataset_part:
        get_request.force_dataset_part = force_dataset_part
    if no_directories:
        get_request.no_directories = no_directories

    if overwrite:
        get_request.overwrite = overwrite
    if skip_existing:
        get_request.skip_existing = skip_existing
    if no_directories:
        get_request.no_directories = no_directories

    if filter_option:
        get_request.regex = filter_to_regex(filter_option)
    if regex:
        get_request.regex = overload_regex_with_additionnal_filter(
            regex, get_request.regex
        )
    if sync or sync_delete:
        get_request.sync = True
        if not get_request.force_dataset_version:
            raise ValueError(
                "Sync requires to set a dataset version. "
                "Please use --dataset-version option."
            )
    if sync_delete:
        get_request.sync_delete = sync_delete
    if index_parts:
        get_request.index_parts = index_parts
        get_request.regex = overload_regex_with_additionnal_filter(
            filter_to_regex("*index_*"), get_request.regex
        )
    if create_file_list is not None:
        assert create_file_list.endswith(".txt") or create_file_list.endswith(
            ".csv"
        ), "Download file list must be a '.txt' or '.csv' file. "
        f"Got '{create_file_list}' instead."
    if file_list_path:
        direct_download_files = get_direct_download_files(file_list_path)
        if direct_download_files:
            get_request.direct_download = direct_download_files
    if create_file_list or dry_run:
        get_request.dry_run = True

    return _run_get_request(
        username=username,
        password=password,
        get_request=get_request,
        create_file_list=create_file_list,
        credentials_file=credentials_file,
        marine_datastore_config=marine_datastore_config,
        max_concurrent_requests=max_concurrent_requests,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )


def _run_get_request(
    username: Optional[str],
    password: Optional[str],
    get_request: GetRequest,
    create_file_list: Optional[str],
    credentials_file: Optional[pathlib.Path],
    marine_datastore_config: MarineDataStoreConfig,
    max_concurrent_requests: int,
    disable_progress_bar: bool,
    staging: bool = False,
) -> ResponseGet:
    logger.debug("Checking username and password...")
    username, password = get_and_check_username_password(
        username, password, credentials_file
    )
    logger.debug("Checking dataset metadata...")

    retrieval_service: RetrievalService = get_retrieval_service(
        dataset_id=get_request.dataset_id,
        force_dataset_version_label=get_request.force_dataset_version,
        force_dataset_part_label=get_request.force_dataset_part,
        force_service_name_or_short_name=None,
        command_type=CommandType.GET,
        dataset_subset=None,
        marine_datastore_config=marine_datastore_config,
    )
    get_request.dataset_url = retrieval_service.uri
    downloaded_files = download_original_files(
        username,
        password,
        get_request,
        max_concurrent_requests,
        disable_progress_bar,
        create_file_list,
    )
    return downloaded_files


def create_get_template() -> None:
    filename = pathlib.Path("get_template.json")
    if filename.exists():
        get_unique_filepath(
            filepath=pathlib.Path("get_template.json"),
        )
    with open(filename, "w") as output_file:
        json.dump(
            {
                "dataset_id": "cmems_mod_ibi_phy_my_0.083deg-3D_P1Y-m",
                "dataset_version": None,
                "dataset_part": None,
                "username": None,
                "password": None,
                "no_directories": False,
                "filter": "*01yav_200[0-2]*",
                "regex": None,
                "output_directory": "copernicusmarine_data",
                "file_list": None,
                "sync": False,
                "sync_delete": False,
                "index_parts": False,
                "disable_progress_bar": False,
                "overwrite": False,
                "log_level": "INFO",
                "dry_run": False,
            },
            output_file,
            indent=4,
        )
    logger.info(f"Template created at: {filename}")


def get_direct_download_files(
    file_list_path: pathlib.Path,
) -> list[str]:
    if not os.path.exists(file_list_path):
        raise FileNotFoundError(
            f"File {file_list_path} does not exist."
            " Please provide a valid path to a '.txt' file."
        )
    with open(file_list_path) as f:
        direct_download_files = [line.strip() for line in f.readlines()]
    return direct_download_files
