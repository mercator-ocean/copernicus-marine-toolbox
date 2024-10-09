import pathlib
from typing import Optional, Union

from copernicusmarine.core_functions import decorators, documentation_utils
from copernicusmarine.core_functions.deprecated import deprecated_python_option
from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
)
from copernicusmarine.core_functions.get import get_function
from copernicusmarine.core_functions.models import ResponseGet
from copernicusmarine.python_interface.exception_handler import (
    log_exception_and_exit,
)


@deprecated_python_option(**DEPRECATED_OPTIONS.dict_old_names_to_new_names)
@log_exception_and_exit
@decorators.docstring_parameter(documentation_utils.GET)
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
    {GET_DESCRIPTION_HELP}

    Parameters
    ----------
    dataset_id : str, optional
        {DATASET_ID_HELP}
    dataset_version : str, optional
        {DATASET_VERSION_HELP}
    dataset_part : str, optional
        {DATASET_PART_HELP}
    username : str, optional
        {USERNAME_HELP}
    password : str, optional
        {PASSWORD_HELP}
    output_directory : Union[pathlib.Path, str], optional
        {OUTPUT_DIRECTORY_HELP}
    credentials_file : Union[pathlib.Path, str], optional
        {CREDENTIALS_FILE_HELP}
    force_download : bool, optional
        {FORCE_DOWNLOAD_HELP}
    overwrite_output_data : bool, optional
        {OVERWRITE_OUTPUT_DATA_HELP}
    request_file : Union[pathlib.Path, str], optional
        {REQUEST_FILE_HELP}
    no_directories : bool, optional
        {NO_DIRECTORIES_HELP}
    show_outputnames : bool, optional
        {SHOW_OUTPUTNAMES_HELP}
    filter : str, optional
        {FILTER_WITH_GLOBBING_PATTERN_HELP}
    regex : str, optional
        {FILTER_WITH_REGULAR_EXPRESSION_HELP}
    file_list : Union[pathlib.Path, str], optional
        {FILE_LIST_HELP}
    create_file_list : str, optional
        {CREATE_FILE_LIST_HELP}
    index_parts : bool, optional
        {INDEX_PARTS_HELP}
    sync : bool, optional
        {SYNC_HELP}
    sync_delete : bool, optional
        {SYNC_DELETE_HELP}
    dry_run : bool, optional
        {DRY_RUN_HELP}
    max_concurrent_requests : int, optional
        {MAX_CONCURRENT_REQUESTS_HELP}
    disable_progress_bar : bool, optional
        {DISABLE_PROGRESS_BAR_HELP}

    Returns
    -------
    {GET_RESPONSE_HELP}
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
