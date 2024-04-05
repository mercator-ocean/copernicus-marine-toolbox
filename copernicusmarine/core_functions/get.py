import json
import logging
import pathlib
from typing import List, Optional

from copernicusmarine.catalogue_parser.catalogue_parser import parse_catalogue
from copernicusmarine.catalogue_parser.request_structure import (
    GetRequest,
    file_list_to_regex,
    filter_to_regex,
    get_request_from_file,
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
from copernicusmarine.core_functions.utils import (
    create_cache_directory,
    delete_cache_folder,
    get_unique_filename,
)
from copernicusmarine.core_functions.versions_verifier import VersionVerifier
from copernicusmarine.download_functions.download_original_files import (
    download_original_files,
)

logger = logging.getLogger("copernicus_marine_root_logger")


def get_function(
    dataset_url: Optional[str],
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
    overwrite_metadata_cache: bool,
    no_metadata_cache: bool,
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

    if overwrite_metadata_cache:
        delete_cache_folder()

    get_request = GetRequest()
    if request_file:
        get_request = get_request_from_file(request_file)
    request_update_dict = {
        "dataset_url": dataset_url,
        "dataset_id": dataset_id,
        "force_dataset_version": force_dataset_version,
        "output_directory": output_directory,
        "force_service": force_service,
    }
    get_request.update(request_update_dict)

    if not no_metadata_cache:
        create_cache_directory()

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
    if file_list_path:
        file_list_regex = file_list_to_regex(file_list_path)
        get_request.regex = overload_regex_with_additionnal_filter(
            file_list_regex, get_request.regex
        )
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

    return _run_get_request(
        username=username,
        password=password,
        get_request=get_request,
        create_file_list=create_file_list,
        credentials_file=credentials_file,
        no_metadata_cache=no_metadata_cache,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )


def _run_get_request(
    username: Optional[str],
    password: Optional[str],
    get_request: GetRequest,
    create_file_list: Optional[str],
    credentials_file: Optional[pathlib.Path],
    no_metadata_cache: bool,
    disable_progress_bar: bool,
    staging: bool = False,
) -> List[pathlib.Path]:
    username, password = get_and_check_username_password(
        username,
        password,
        credentials_file,
        no_metadata_cache=no_metadata_cache,
    )
    catalogue = parse_catalogue(
        no_metadata_cache=no_metadata_cache,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )
    retrieval_service: RetrievalService = get_retrieval_service(
        catalogue,
        get_request.dataset_id,
        get_request.dataset_url,
        get_request.force_dataset_version,
        get_request.force_dataset_part,
        get_request.force_service,
        CommandType.GET,
        get_request.index_parts,
        dataset_sync=get_request.sync,
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
                "overwrite_metadata_cache": False,
                "no_metadata_cache": False,
                "log_level": "INFO",
            },
            output_file,
            indent=4,
        )
    logger.info(f"Template created at: {filename}")
