import pathlib
from typing import Optional, Union

from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
)
from copernicusmarine.core_functions.get import get_function
from copernicusmarine.core_functions.models import ResponseGet
from copernicusmarine.core_functions.utils import deprecated_python_option
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)


@deprecated_python_option(**DEPRECATED_OPTIONS.dict_old_names_to_new_names)
@log_exception_and_exit
def get(
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
    filter: Optional[str] = None,
    regex: Optional[str] = None,
    file_list: Optional[Union[pathlib.Path, str]] = None,
    create_file_list: Optional[str] = None,
    index_parts: bool = False,
    sync: bool = False,
    sync_delete: bool = False,
    dry_run: bool = False,
    max_concurrent_requests: int = 15,
    disable_progress_bar: bool = False,
    staging: bool = False,
) -> ResponseGet:
    """
    Download originally produced data files.

    The datasetID is required (either as an argument or in a request file) and can be found via the ``describe`` command.

    Parameters
    ----------
    dataset_id : str, optional
        The datasetID, required either as an argument or in the request_file option.
    dataset_version : str, optional
        Force the selection of a specific dataset version.
    dataset_part : str, optional
        Force the selection of a specific dataset part.
    username : str, optional
        The username for authentication.
    password : str, optional
        The password for authentication.
    output_directory : Union[pathlib.Path, str], optional
        The destination folder for the downloaded files. Default is the current directory.
    credentials_file : Union[pathlib.Path, str], optional
        Path to a credentials file if not in its default directory (``$HOME/.copernicusmarine``). Accepts .copernicusmarine-credentials / .netrc or _netrc / motuclient-python.ini files.
    force_download : bool, optional
        Flag to skip confirmation before download.
    overwrite_output_data : bool, optional
        If specified and if the file already exists on destination, then it will be overwritten instead of creating new one with unique index.
    request_file : Union[pathlib.Path, str], optional
        Option to pass a file containing the arguments. For more information please refer to the documentation or use option ``--create-template`` from the command line interface for an example template.
    no_directories : bool, optional
        If True, downloaded files will not be organized into directories.
    show_outputnames : bool, optional
        Option to display the names of the output files before download.
    filter : str, optional
        A pattern that must match the absolute paths of the files to download.
    regex : str, optional
        The regular expression that must match the absolute paths of the files to download.
    file_list : Union[pathlib.Path, str], optional
        Path to a '.txt' file containing a list of file paths, line by line, that will be downloaded directly. These files must be from the same dataset as the one specified dataset with the datasetID option. If no files can be found, the Toolbox will list all files on the remote server and attempt to find a match.
    create_file_list : str, optional
        Option to only create a file containing the names of the targeted files instead of downloading them. It writes the file to the specified output directory (default to current directory). The file name specified should end with '.txt' or '.csv'. If specified, no other action will be performed.
    index_parts : bool, optional
        Option to get the index files of an INSITU dataset.
    sync : bool, optional
        Option to synchronize the local directory with the remote directory. See the documentation for more details.
    sync_delete : bool, optional
        Option to delete local files that are not present on the remote server while applying sync.
    dry_run : bool, optional
        If True, runs query without downloading data.
    max_concurrent_requests : int, optional
        Maximum number of concurrent requests. Default 15. The command uses a thread pool executor to manage concurrent requests. If set to 0, no parallel executions are used.
    disable_progress_bar : bool, optional
        Flag to hide progress bar.

    Returns
    -------
    ResponseGet
        A list of files that were downloaded and some metadata.

    """  # noqa
    output_directory = (
        pathlib.Path(output_directory) if output_directory else None
    )
    credentials_file = (
        pathlib.Path(credentials_file) if credentials_file else None
    )
    file_list = pathlib.Path(file_list) if file_list else None
    request_file = pathlib.Path(request_file) if request_file else None
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
        filter=filter,
        regex=regex,
        file_list_path=file_list,
        create_file_list=create_file_list,
        index_parts=index_parts,
        sync=sync,
        sync_delete=sync_delete,
        dry_run=dry_run,
        max_concurrent_requests=max_concurrent_requests,
        disable_progress_bar=disable_progress_bar,
        staging=staging,
    )
