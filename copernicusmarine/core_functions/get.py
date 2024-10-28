import json
import logging
import os
import pathlib
from typing import List, Optional

from copernicusmarine.catalogue_parser.request_structure import (
    GetRequest,
    filter_to_regex,
    overload_regex_with_additionnal_filter,
)
from copernicusmarine.core_functions.credentials_utils import (
    get_and_check_username_password,
)
from copernicusmarine.core_functions.services_utils import (
    CommandType,
    RetrievalService,
    get_retrieval_service,
)
from copernicusmarine.core_functions.utils import get_unique_filename
from copernicusmarine.core_functions.versions_verifier import VersionVerifier
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
    show_outputnames: bool,
    output_directory: Optional[pathlib.Path],
    credentials_file: Optional[pathlib.Path],
    force_download: bool,
    overwrite_output_data: bool,
    request_file: Optional[pathlib.Path],
    force_service: Optional[str],
    filter: Optional[str],
    regex: Optional[str],
    file_list_path: Optional[pathlib.Path],
    create_file_list: Optional[str],
    download_file_list: bool,
    sync: bool,
    sync_delete: bool,
    index_parts: bool,
    disable_progress_bar: bool,
    staging: bool,
) -> List[pathlib.Path]:
    VersionVerifier.check_version_get(staging)
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
        "force_service": force_service,
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
    if show_outputnames:
        get_request.show_outputnames = show_outputnames
    if force_download:
        get_request.force_download = force_download
    if overwrite_output_data:
        get_request.overwrite_output_data = overwrite_output_data
    if force_service:
        get_request.force_service = force_service
    if filter:
        get_request.regex = filter_to_regex(filter)
    if regex:
        get_request.regex = overload_regex_with_additionnal_filter(
            regex, get_request.regex
        )
    if sync or sync_delete:
        get_request.sync = True
        if not get_request.force_dataset_version:
            raise ValueError(
                "Sync requires to set a dataset version. "
                "Please use --force-dataset-version option."
            )
    if sync_delete:
        get_request.sync_delete = sync_delete
    if index_parts:
        get_request.index_parts = index_parts
        get_request.force_service = "files"
        get_request.regex = overload_regex_with_additionnal_filter(
            filter_to_regex("*index_*"), get_request.regex
        )
    if download_file_list and not create_file_list:
        create_file_list = "files_to_download.txt"
    if create_file_list is not None:
        assert create_file_list.endswith(".txt") or create_file_list.endswith(
            ".csv"
        ), "Download file list must be a .txt or .csv file. "
        f"Got '{create_file_list}' instead."
    if file_list_path:
        direct_download_files = get_direct_download_files(file_list_path)
        if direct_download_files:
            get_request.direct_download = direct_download_files

    return _run_get_request(
        username=username,
        password=password,
        get_request=get_request,
        create_file_list=create_file_list,
        credentials_file=credentials_file,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )


def _run_get_request(
    username: Optional[str],
    password: Optional[str],
    get_request: GetRequest,
    create_file_list: Optional[str],
    credentials_file: Optional[pathlib.Path],
    disable_progress_bar: bool,
    staging: bool = False,
) -> List[pathlib.Path]:
    logger.debug("Checking username and password...")
    username, password = get_and_check_username_password(
        username, password, credentials_file
    )
    logger.debug("Checking dataset metadata...")

    retrieval_service: RetrievalService = get_retrieval_service(
        get_request.dataset_id,
        get_request.force_dataset_version,
        get_request.force_dataset_part,
        get_request.force_service,
        CommandType.GET,
        get_request.index_parts,
        dataset_sync=get_request.sync,
        staging=staging,
    )
    get_request.dataset_url = retrieval_service.uri
    logger.info(
        "Downloading using service "
        f"{retrieval_service.service_type.service_name.value}..."
    )
    downloaded_files = download_original_files(
        username,
        password,
        get_request,
        disable_progress_bar,
        create_file_list,
    )
    logger.debug(downloaded_files)
    return downloaded_files


def create_get_template() -> None:
    filename = get_unique_filename(
        filepath=pathlib.Path("get_template.json"), overwrite_option=False
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
                "show_outputnames": True,
                "service": "files",
                "force_download": False,
                "file_list": None,
                "sync": False,
                "sync_delete": False,
                "index_parts": False,
                "disable_progress_bar": False,
                "overwrite_output_data": False,
                "log_level": "INFO",
            },
            output_file,
            indent=4,
        )
    logger.info(f"Template created at: {filename}")


def get_direct_download_files(
    file_list_path: Optional[pathlib.Path],
) -> Optional[list[str]]:
    if file_list_path:
        if not os.path.exists(file_list_path):
            raise FileNotFoundError(
                f"File {file_list_path} does not exist."
                " Please provide a valid path to a .txt file."
            )
        with open(file_list_path) as f:
            direct_download_files = [line.strip() for line in f.readlines()]
        return direct_download_files
    else:
        return None
