import datetime
import logging
import os
import pathlib
import re
from itertools import chain
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import List, Optional, Tuple

import boto3
import botocore
import click
from numpy import append, arange
from tqdm import tqdm

from copernicusmarine.catalogue_parser.request_structure import GetRequest
from copernicusmarine.core_functions.utils import (
    FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE,
    construct_query_params_for_marine_data_store_monitoring,
    construct_url_with_query_params,
    flatten,
    get_unique_filename,
    parse_access_dataset_url,
)

logger = logging.getLogger("copernicus_marine_root_logger")


def download_original_files(
    username: str,
    password: str,
    get_request: GetRequest,
    disable_progress_bar: bool,
    create_file_list: Optional[str],
) -> list[pathlib.Path]:
    result = _download_header(
        str(get_request.dataset_url),
        get_request.regex,
        username,
        password,
        get_request.sync,
        create_file_list,
        pathlib.Path(get_request.output_directory),
        only_list_root_path=get_request.index_parts,
        overwrite=get_request.overwrite_output_data,
    )
    if result is None:
        return []
    (
        message,
        locator,
        filenames_in,
        total_size,
        filenames_in_sync_ignored,
    ) = result
    filenames_out = create_filenames_out(
        filenames_in=filenames_in,
        output_directory=pathlib.Path(get_request.output_directory),
        no_directories=get_request.no_directories,
        overwrite=(
            get_request.overwrite_output_data
            if not get_request.sync
            else False
        ),
    )
    if not get_request.force_download and total_size:
        logger.info(message)
    if get_request.show_outputnames:
        logger.info("Output filenames:")
        for filename_out in filenames_out:
            logger.info(filename_out)
    files_to_delete = []
    if get_request.sync_delete:
        filenames_out_sync_ignored = create_filenames_out(
            filenames_in=filenames_in_sync_ignored,
            output_directory=pathlib.Path(get_request.output_directory),
            no_directories=get_request.no_directories,
            overwrite=False,
            unique_names_compared_to_local_files=False,
        )
        files_to_delete = _get_files_to_delete_with_sync(
            filenames_in=filenames_in_sync_ignored,
            output_directory=pathlib.Path(get_request.output_directory),
            filenames_out=filenames_out_sync_ignored,
        )
        if files_to_delete:
            logger.info("Some files will be deleted due to sync delete:")
            for file_to_delete in files_to_delete:
                logger.info(file_to_delete)
    if not total_size:
        logger.info("No data to download")
        if not files_to_delete:
            return []
    if not get_request.force_download:
        click.confirm(
            FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE, default=True, abort=True
        )
    endpoint: str
    bucket: str
    endpoint, bucket = locator
    if get_request.sync_delete and files_to_delete:
        for file_to_delete in files_to_delete:
            file_to_delete.unlink()
    return download_files(
        username,
        endpoint,
        bucket,
        filenames_in,
        filenames_out,
        disable_progress_bar,
    )


def _get_files_to_delete_with_sync(
    filenames_in: list[str],
    output_directory: pathlib.Path,
    filenames_out: list[Path],
) -> list[pathlib.Path]:
    product_structure = str(
        _local_path_from_s3_url(filenames_in[0], Path(""))
    ).split("/")
    product_id = product_structure[0]
    dataset_id = product_structure[1]
    dataset_level_local_folder = output_directory / product_id / dataset_id
    files_to_delete = []
    for local_file in dataset_level_local_folder.glob("**/*"):
        if local_file.is_file() and local_file not in filenames_out:
            files_to_delete.append(local_file)
    return files_to_delete


def download_files(
    username: str,
    endpoint_url: str,
    bucket: str,
    filenames_in: List[str],
    filenames_out: List[pathlib.Path],
    disable_progress_bar: bool,
) -> list[pathlib.Path]:
    pool = ThreadPool()
    nfiles_per_process, nfiles = 1, len(filenames_in)
    indexes = append(
        arange(0, nfiles, nfiles_per_process, dtype=int),
        nfiles,
    )
    groups_in_files = [
        filenames_in[indexes[i] : indexes[i + 1]]
        for i in range(len(indexes) - 1)
    ]
    groups_out_files = [
        filenames_out[indexes[i] : indexes[i + 1]]
        for i in range(len(indexes) - 1)
    ]

    for groups_out_file in groups_out_files:
        parent_dir = Path(groups_out_file[0]).parent
        if not parent_dir.is_dir():
            pathlib.Path.mkdir(parent_dir, parents=True)

    download_summary_list = pool.imap(
        _download_files,
        zip(
            [username] * len(groups_in_files),
            [endpoint_url] * len(groups_in_files),
            [bucket] * len(groups_in_files),
            groups_in_files,
            groups_out_files,
        ),
    )
    download_summary = list(
        tqdm(
            download_summary_list,
            total=len(groups_in_files),
            disable=disable_progress_bar,
        )
    )
    return flatten(download_summary)


def _download_header(
    data_path: str,
    regex: Optional[str],
    username: str,
    _password: str,
    sync: bool,
    create_file_list: Optional[str],
    directory_out: pathlib.Path,
    only_list_root_path: bool = False,
    overwrite: bool = False,
) -> Optional[Tuple[str, Tuple[str, str], list[str], float, list[str]]]:
    (endpoint_url, bucket, path) = parse_access_dataset_url(
        data_path, only_dataset_root_path=only_list_root_path
    )

    filenames, sizes, total_size = [], [], 0.0
    raw_filenames = _list_files_on_marine_data_lake_s3(
        username, endpoint_url, bucket, path, not only_list_root_path
    )
    filename_filtered = []
    filenames_without_sync = []
    for filename, size, last_modified_datetime, etag in raw_filenames:
        if not regex or re.search(regex, filename):
            filenames_without_sync.append(filename)
            if not sync or _check_needs_to_be_synced(
                filename, size, last_modified_datetime, directory_out
            ):
                filenames.append(filename)
                sizes.append(float(size))
                total_size += float(size)
                filename_filtered.append(
                    (filename, size, last_modified_datetime, etag)
                )

    if create_file_list and create_file_list.endswith(".txt"):
        download_filename = get_unique_filename(
            directory_out / create_file_list, overwrite
        )
        logger.info(f"The file list is written at {download_filename}")
        with open(download_filename, "w") as file_out:
            for filename, _, _, _ in filename_filtered:
                file_out.write(f"{filename}\n")
        return None
    elif create_file_list and create_file_list.endswith(".csv"):
        download_filename = get_unique_filename(
            directory_out / create_file_list, overwrite
        )
        logger.info(f"The file list is written at {download_filename}")
        with open(download_filename, "w") as file_out:
            file_out.write("filename,size,last_modified_datetime,etag\n")
            for (
                filename,
                size,
                last_modified_datetime,
                etag,
            ) in filename_filtered:
                file_out.write(
                    f"{filename},{size},{last_modified_datetime},{etag}\n"
                )
        return None

    message = "You requested the download of the following files:\n"
    for filename, size, last_modified_datetime, _ in filename_filtered[:20]:
        message += str(filename)
        datetime_iso = re.sub(
            r"\+00:00$",
            "Z",
            last_modified_datetime.astimezone(datetime.timezone.utc).isoformat(
                timespec="seconds"
            ),
        )
        message += f" - {format_file_size(float(size))} - {datetime_iso}\n"
    if len(filenames) > 20:
        message += f"Printed 20 out of {len(filenames)} files\n"
    message += (
        f"\nTotal size of the download: {format_file_size(total_size)}\n\n"
    )
    locator = (endpoint_url, bucket)
    return (message, locator, filenames, total_size, filenames_without_sync)


def _check_needs_to_be_synced(
    filename: str,
    size: int,
    last_modified_datetime: datetime.datetime,
    directory_out: pathlib.Path,
) -> bool:
    filename_out = _local_path_from_s3_url(filename, directory_out)
    if not filename_out.is_file():
        return True
    else:
        file_stats = filename_out.stat()
        if file_stats.st_size != size:
            return True
        else:
            last_created_datetime_out = datetime.datetime.fromtimestamp(
                file_stats.st_ctime, tz=datetime.timezone.utc
            )
            return last_modified_datetime > last_created_datetime_out


def _local_path_from_s3_url(
    s3_url: str, local_directory: pathlib.Path
) -> pathlib.Path:
    return local_directory / pathlib.Path("/".join(s3_url.split("/")[4:]))


def _list_files_on_marine_data_lake_s3(
    username: str,
    endpoint_url: str,
    bucket: str,
    prefix: str,
    recursive: bool,
) -> list[tuple[str, int, datetime.datetime, str]]:
    def _add_custom_query_param(params, context, **kwargs):
        """
        Add custom query params for MDS's Monitoring
        """
        params["url"] = construct_url_with_query_params(
            params["url"],
            construct_query_params_for_marine_data_store_monitoring(username),
        )

    s3_session = boto3.Session()
    s3_client = s3_session.client(
        "s3",
        config=botocore.config.Config(
            # Configures to use subdomain/virtual calling format.
            s3={"addressing_style": "virtual"},
            signature_version=botocore.UNSIGNED,
        ),
        endpoint_url=endpoint_url,
    )

    # Register the botocore event handler for adding custom query params
    # to S3 LIST requests
    s3_client.meta.events.register(
        "before-call.s3.ListObjects", _add_custom_query_param
    )

    paginator = s3_client.get_paginator("list_objects")
    page_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter="/" if not recursive else "",
    )

    s3_objects = chain(
        *map(lambda page: page.get("Contents", []), page_iterator)
    )

    files_already_found: list[tuple[str, int, datetime.datetime, str]] = []
    for s3_object in s3_objects:
        files_already_found.append(
            (
                f"s3://{bucket}/" + s3_object["Key"],
                s3_object["Size"],
                s3_object["LastModified"],
                s3_object["ETag"],
            )
        )
    return files_already_found


def _download_files(
    tuple_original_files_filename: Tuple[
        str, str, str, list[str], list[pathlib.Path]
    ],
) -> list[pathlib.Path]:
    (
        username,
        endpoint_url,
        bucket,
        filenames_in,
        filenames_out,
    ) = tuple_original_files_filename

    def _add_custom_query_param(params, context, **kwargs):
        """
        Add custom query params for MDS's Monitoring
        """
        params["url"] = construct_url_with_query_params(
            params["url"],
            construct_query_params_for_marine_data_store_monitoring(username),
        )

    def _original_files_file_download(
        endpoint_url: str, bucket: str, file_in: str, file_out: pathlib.Path
    ) -> pathlib.Path:
        """
        Download ONE file and return a string of the result
        """
        s3_session = boto3.Session()
        s3_client = s3_session.client(
            "s3",
            config=botocore.config.Config(
                # Configures to use subdomain/virtual calling format.
                s3={"addressing_style": "virtual"},
                signature_version=botocore.UNSIGNED,
            ),
            endpoint_url=endpoint_url,
        )
        s3_resource = boto3.resource(
            "s3",
            config=botocore.config.Config(
                # Configures to use subdomain/virtual calling format.
                s3={"addressing_style": "virtual"},
                signature_version=botocore.UNSIGNED,
            ),
            endpoint_url=endpoint_url,
        )

        # Register the botocore event handler for adding custom query params
        # to S3 HEAD and GET requests
        s3_client.meta.events.register(
            "before-call.s3.HeadObject", _add_custom_query_param
        )
        s3_client.meta.events.register(
            "before-call.s3.GetObject", _add_custom_query_param
        )

        last_modified_date_epoch = s3_resource.Object(
            bucket, file_in.replace(f"s3://{bucket}/", "")
        ).last_modified.timestamp()

        s3_client.download_file(
            bucket,
            file_in.replace(f"s3://{bucket}/", ""),
            file_out,
        )

        os.utime(
            file_out, (last_modified_date_epoch, last_modified_date_epoch)
        )

        return file_out

    download_summary = []
    for file_in, file_out in zip(filenames_in, filenames_out):
        download_summary.append(
            _original_files_file_download(
                endpoint_url, bucket, file_in, file_out
            )
        )
    return download_summary


# /////////////////////////////
# --- Tools
# /////////////////////////////


def create_filenames_out(
    filenames_in: list[str],
    overwrite: bool,
    output_directory: pathlib.Path = pathlib.Path("."),
    no_directories=False,
    unique_names_compared_to_local_files=True,
) -> list[pathlib.Path]:
    filenames_out = []
    for filename_in in filenames_in:
        if no_directories:
            filename_out = (
                pathlib.Path(output_directory) / pathlib.Path(filename_in).name
            )
        else:
            # filename_in: s3://mdl-native-xx/native/<product-id>..
            filename_out = _local_path_from_s3_url(
                filename_in, output_directory
            )
        if unique_names_compared_to_local_files:
            filename_out = get_unique_filename(
                filepath=filename_out, overwrite_option=overwrite
            )

        filenames_out.append(filename_out)
    return filenames_out


def format_file_size(
    size: float, decimals: int = 2, binary_system: bool = False
) -> str:
    if binary_system:
        units: list[str] = [
            "B",
            "KiB",
            "MiB",
            "GiB",
            "TiB",
            "PiB",
            "EiB",
            "ZiB",
        ]
        largest_unit: str = "YiB"
        step: int = 1024
    else:
        units = ["B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB"]
        largest_unit = "YB"
        step = 1000

    for unit in units:
        if size < step:
            return ("%." + str(decimals) + "f %s") % (size, unit)
        size /= step

    return ("%." + str(decimals) + "f %s") % (size, largest_unit)
