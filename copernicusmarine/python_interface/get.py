import pathlib
from typing import List, Optional, Union

from copernicusmarine.core_functions.deprecated import (
    deprecated_python_option,
    log_deprecated_message,
    raise_both_old_and_new_value_error,
)
from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
)
from copernicusmarine.core_functions.get import get_function
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)


@deprecated_python_option(DEPRECATED_OPTIONS)
@log_exception_and_exit
def get(
    dataset_id: Optional[str],
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
    filter: Optional[str] = None,
    regex: Optional[str] = None,
    file_list: Optional[Union[pathlib.Path, str]] = None,
    create_file_list: Optional[str] = None,
    download_file_list: bool = False,
    index_parts: bool = False,
    sync: bool = False,
    sync_delete: bool = False,
    disable_progress_bar: bool = False,
    staging: bool = False,
) -> List[pathlib.Path]:
    """
    Fetches data from the Copernicus Marine server based on the provided parameters.

    :param dataset_id: The unique identifier of the dataset.
    :type dataset_id: str, optional
    :param dataset_version: Force the use of a specific dataset version.
    :type dataset_version: str, optional
    :param dataset_part: Force the use of a specific dataset part.
    :type dataset_part: str, optional
    :param username: The username for authentication. See also :func:`~copernicusmarine.login`.
    :type username: str, optional
    :param password: The password for authentication. See also :func:`~copernicusmarine.login`.
    :type password: str, optional
    :param output_directory: The directory where downloaded files will be saved.
    :type output_directory: Union[pathlib.Path, str], optional
    :param credentials_file: Path to a file containing authentication credentials.
    :type credentials_file: Union[pathlib.Path, str], optional
    :param force_download: Skip confirmation before download.
    :type force_download: bool, optional
    :param overwrite_output_data: If True, overwrite existing output files.
    :type overwrite_output_data: bool, optional
    :param request_file: Path to a file containing request parameters. For more information, please refer to the README.
    :type request_file: Union[pathlib.Path, str], optional
    :param service: Force the use of a specific service.
    :type service: str, optional
    :param no_directories: If True, downloaded files will not be organized into directories.
    :type no_directories: bool, optional
    :param show_outputnames: If True, display the names of the downloaded files.
    :type show_outputnames: bool, optional
    :param filter: Apply a filter to the downloaded data.
    :type filter: str, optional
    :param regex: Apply a regular expression filter to the downloaded data.
    :type regex: str, optional
    :param file_list: Path to a .txt file containing a list of file paths, line by line, that will be downloaded directly. These files must be from the specified dataset using the --dataset-id. If no files can be found, the Toolbox will list all files on the remote server and attempt to find a match.
    :type file_list: Union[pathlib.Path, str], optional
    :param create_file_list: Option to only create a file containing the names of the targeted files instead of downloading them. It writes the file in the directory specified with the --output-directory option (default to current directory). If specified, no other action will be performed.
    :type create_file_list: str, optional
    :param index_parts: If True, download index files. Only for INSITU datasets. Temporary option.
    :type index_parts: bool, optional
    :param sync: If True, synchronize the local directory with the remote directory.
    :type sync: bool, optional
    :param sync_delete: If True, delete local files that are not present on the remote server while applying sync.
    :type sync_delete: bool, optional

    :returns: A list of paths to the downloaded files.
    :rtype: List[pathlib.Path]
    """  # noqa
    output_directory = (
        pathlib.Path(output_directory) if output_directory else None
    )
    credentials_file = (
        pathlib.Path(credentials_file) if credentials_file else None
    )
    file_list = pathlib.Path(file_list) if file_list else None
    request_file = pathlib.Path(request_file) if request_file else None
    if download_file_list and create_file_list:
        raise_both_old_and_new_value_error(
            "download_file_list", "create_file_list"
        )
    elif download_file_list:
        log_deprecated_message(
            "download_file_list",
            "create_file_list",
            deleted_for_v2=True,
            deprecated_for_v2=False,
            only_for_v2=False,
        )
    return get_function(
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
        filter=filter,
        regex=regex,
        file_list_path=file_list,
        create_file_list=create_file_list,
        download_file_list=download_file_list,
        index_parts=index_parts,
        sync=sync,
        sync_delete=sync_delete,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )
