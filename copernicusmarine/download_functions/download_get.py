import logging
import pathlib
from typing import Any, Callable, List, Optional, Tuple

import click

from copernicusmarine.catalogue_parser.request_structure import GetRequest
from copernicusmarine.core_functions.utils import (
    FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE,
)

logger = logging.getLogger("copernicus_marine_root_logger")


def download_get(
    username: str,
    password: str,
    get_request: GetRequest,
    download_file_list: Optional[str],
    download_header: Callable,
    create_filenames_out: Callable,
) -> Optional[Tuple[List[str], List[pathlib.Path], Any]]:
    # locator can be a hostname, a tuple with endpoint + bucket, etc.
    message, locator, filenames_in, total_size = download_header(
        str(get_request.dataset_url),
        get_request.regex,
        username,
        password,
        pathlib.Path(get_request.output_directory),
        download_file_list,
        overwrite=get_request.overwrite_output_data,
    )
    filenames_out = create_filenames_out(
        filenames_in=filenames_in,
        output_directory=get_request.output_directory,
        no_directories=get_request.no_directories,
        overwrite=get_request.overwrite_output_data,
    )
    if not total_size:
        logger.info("No data to download")
        return None
    if not get_request.force_download:
        logger.info(message)
    if get_request.show_outputnames:
        logger.info("Output filenames:")
        for filename_out in filenames_out:
            logger.info(filename_out)
    if not get_request.force_download:
        click.confirm(
            FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE, default=True, abort=True
        )
    return (filenames_in, filenames_out, locator)
