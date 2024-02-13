import pathlib
from typing import List, Optional, Union

from copernicusmarine.core_functions.deprecated import deprecated_python_option
from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
)
from copernicusmarine.core_functions.get import get_function
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)


@deprecated_python_option(**DEPRECATED_OPTIONS.dict_old_names_to_new_names)
@log_exception_and_exit
def get(
    dataset_url: Optional[str] = None,
    dataset_id: Optional[str] = None,
    dataset_version: Optional[str] = None,
    dataset_part: Optional[str] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    no_directories: bool = False,
    show_outputnames: bool = False,
    output_directory: Optional[Union[pathlib.Path, str]] = None,
    credentials_file: Optional[Union[pathlib.Path, str]] = None,
    force_download: bool = False,
    overwrite_output_data: bool = False,
    request_file: Optional[Union[pathlib.Path, str]] = None,
    service: Optional[str] = None,
    overwrite_metadata_cache: bool = False,
    no_metadata_cache: bool = False,
    filter: Optional[str] = None,
    regex: Optional[str] = None,
    sync: bool = False,
    sync_delete: bool = False,
    disable_progress_bar: bool = False,
    staging: bool = False,
) -> List[pathlib.Path]:
    """
    Fetches data from the Copernicus Marine server based on the provided parameters.

    Args:
        dataset_url (str, optional): The URL of the dataset to retrieve.
        dataset_id (str, optional): The unique identifier of the dataset.
        dataset_version (str, optional): Force the use of a specific dataset version.
        dataset_part (str, optional): Force the use of a specific dataset part.
        username (str, optional): The username for authentication.
        password (str, optional): The password for authentication.
        no_directories (bool, optional): If True, downloaded files will not be organized into directories.
        show_outputnames (bool, optional): If True, display the names of the downloaded files.
        output_directory (Union[pathlib.Path, str], optional): The directory where downloaded files will be saved.
        credentials_file (Union[pathlib.Path, str], optional): Path to a file containing authentication credentials.
        force_download (bool, optional): Skip confirmation before download.
        overwrite_output_data (bool, optional): If True, overwrite existing output files.
        request_file (Union[pathlib.Path, str], optional): Path to a file containing request parameters.
        service (str, optional): Force the use of a specific service.
        overwrite_metadata_cache (bool, optional): If True, overwrite the metadata cache.
        no_metadata_cache (bool, optional): If True, do not use the metadata cache.
        filter (str, optional): Apply a filter to the downloaded data.
        regex (str, optional): Apply a regular expression filter to the downloaded data.
        sync (bool, optional): If True, synchronize the local directory with the remote directory.
        sync_delete (bool, optional): If True, delete local files that are not present on the remote server while applying sync.

    Returns:
        List[pathlib.Path]: A list of paths to the downloaded files.
    """  # noqa
    output_directory = (
        pathlib.Path(output_directory) if output_directory else None
    )
    credentials_file = (
        pathlib.Path(credentials_file) if credentials_file else None
    )
    request_file = pathlib.Path(request_file) if request_file else None
    return get_function(
        dataset_url,
        dataset_id,
        dataset_version,
        dataset_part,
        username,
        password,
        no_directories,
        show_outputnames,
        output_directory,
        credentials_file,
        force_download,
        overwrite_output_data,
        request_file,
        service,
        overwrite_metadata_cache,
        no_metadata_cache,
        filter,
        regex,
        sync,
        sync_delete,
        disable_progress_bar,
        staging=staging,
    )
