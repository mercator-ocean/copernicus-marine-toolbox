import json
import logging
import os
import pathlib
from typing import Optional

from copernicusmarine.core_functions.marine_datastore_config import (
    MarineDataStoreConfig,
    get_config_and_check_version_get,
)
from copernicusmarine.core_functions.models import CommandType, ResponseGet
from copernicusmarine.core_functions.request_structure import GetRequest
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
    get_request: GetRequest,
    staging: bool,
) -> ResponseGet:
    marine_datastore_config = get_config_and_check_version_get(staging)
    if staging:
        logger.warning(
            "Detecting staging flag for get command. "
            "Data will come from the staging environment."
        )
    return _run_get_request(
        get_request=get_request,
        create_file_list=get_request.create_file_list,
        marine_datastore_config=marine_datastore_config,
        max_concurrent_requests=get_request.max_concurrent_requests,
        disable_progress_bar=get_request.disable_progress_bar,
    )


def _run_get_request(
    get_request: GetRequest,
    create_file_list: Optional[str],
    marine_datastore_config: MarineDataStoreConfig,
    max_concurrent_requests: int,
    disable_progress_bar: bool,
) -> ResponseGet:
    logger.debug("Checking dataset metadata...")

    retrieval_service: RetrievalService = get_retrieval_service(
        request=get_request,
        command_type=CommandType.GET,
        marine_datastore_config=marine_datastore_config,
    )
    get_request.dataset_url = retrieval_service.uri
    downloaded_files = download_original_files(
        get_request.username,
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
                "dataset_id": "cmems_mod_ibi_phy-temp_my_0.027deg_P1Y-m",
                "dataset_version": None,
                "dataset_part": None,
                "username": None,
                "password": None,
                "no_directories": False,
                "filter": "*01yav_temp_200[0-2]*",
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
