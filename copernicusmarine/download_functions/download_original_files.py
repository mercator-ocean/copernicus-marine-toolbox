import datetime
import logging
import os
import pathlib
import re
from itertools import chain
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Iterator, List, Optional, Tuple

import click
from botocore.client import ClientError
from numpy import append, arange
from tqdm import tqdm

from copernicusmarine.catalogue_parser.request_structure import (
    GetRequest,
    overload_regex_with_additionnal_filter,
)
from copernicusmarine.core_functions.environment_variables import (
    COPERNICUSMARINE_GET_CONCURRENT_DOWNLOADS,
)
from copernicusmarine.core_functions.sessions import (
    get_configured_boto3_session,
)
from copernicusmarine.core_functions.utils import (
    FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE,
    flatten,
    get_unique_filename,
    parse_access_dataset_url,
)

logger = logging.getLogger("copernicus_marine_root_logger")
blank_logger = logging.getLogger("copernicus_marine_blank_logger")

NUMBER_THREADS = (
    int(COPERNICUSMARINE_GET_CONCURRENT_DOWNLOADS)
    if COPERNICUSMARINE_GET_CONCURRENT_DOWNLOADS
    else None
)


def download_original_files(
    username: str,
    password: str,
    get_request: GetRequest,
    disable_progress_bar: bool,
    create_file_list: Optional[str],
) -> list[pathlib.Path]:
    files_not_found: list[str] = []
    filenames_in_sync_ignored: list[str] = []
    total_size: float = 0.0
    sizes: list[float] = []
    last_modified_datetimes: list[datetime.datetime] = []
    filenames_in: list[str] = []
    if get_request.direct_download:
        (
            locator,
            filenames_in,
            sizes,
            last_modified_datetimes,
            total_size,
            filenames_in_sync_ignored,
            files_not_found,
        ) = _download_header_for_direct_download(
            get_request.direct_download,
            str(get_request.dataset_url),
            get_request.sync,
            pathlib.Path(get_request.output_directory),
            username,
        )
    if not get_request.direct_download or files_not_found or get_request.regex:
        if files_not_found:
            files_not_found_regex = "|".join(
                [
                    re.escape(file_not_found)
                    for file_not_found in files_not_found
                ]
            )
            get_request.regex = overload_regex_with_additionnal_filter(
                files_not_found_regex, get_request.regex
            )
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
        if result:
            (
                locator,
                filenames_in_listing,
                sizes_listing,
                last_modified_datetimes_listing,
                total_size_listing,
                filenames_in_sync_ignored_listing,
            ) = result
            filenames_in.extend(filenames_in_listing)
            filenames_in_sync_ignored.extend(filenames_in_sync_ignored_listing)
            total_size += total_size_listing
            sizes.extend(sizes_listing)
            last_modified_datetimes.extend(last_modified_datetimes_listing)
        elif not get_request.direct_download or len(files_not_found) == len(
            get_request.direct_download
        ):
            return []
    message = _create_information_message_before_download(
        filenames_in, sizes, last_modified_datetimes, total_size
    )
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
        for filename_out in filenames_out:
            blank_logger.info(filename_out)
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
            FORCE_DOWNLOAD_CLI_PROMPT_MESSAGE,
            default=True,
            abort=True,
            err=True,
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

    # TODO: v2 It would be proably better to use an async approach
    # TODO: v2 probably better to use an argument for the number
    # of threads instead of using the environment variable
    if NUMBER_THREADS is None or NUMBER_THREADS:
        pool = ThreadPool(processes=NUMBER_THREADS)
        download_summary_list: Iterator[List[Path]] = pool.imap(
            _download_files,
            zip(
                [username] * len(groups_in_files),
                [endpoint_url] * len(groups_in_files),
                [bucket] * len(groups_in_files),
                groups_in_files,
                groups_out_files,
            ),
        )
    else:
        logger.info("Downloading files one by one...")
        download_summary_list = map(
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
) -> Optional[
    Tuple[
        Tuple[str, str],
        list[str],
        List[float],
        List[datetime.datetime],
        float,
        list[str],
    ]
]:
    (endpoint_url, bucket, path) = parse_access_dataset_url(
        data_path, only_dataset_root_path=only_list_root_path
    )

    filenames: list[str] = []
    sizes: list[float] = []
    total_size = 0.0
    last_modified_datetimes: list[datetime.datetime] = []
    etags: list[str] = []
    raw_filenames = _list_files_on_marine_data_lake_s3(
        username, endpoint_url, bucket, path, not only_list_root_path
    )
    filenames_without_sync = []
    for filename, size, last_modified_datetime, etag in raw_filenames:
        if not regex or re.search(regex, filename):
            filenames_without_sync.append(filename)
            if not sync or _check_needs_to_be_synced(
                filename, size, last_modified_datetime, directory_out
            ):
                filenames.append(filename)
                sizes.append(float(size))
                last_modified_datetimes.append(last_modified_datetime)
                etags.append(etag)
    total_size = sum(sizes)
    if create_file_list and create_file_list.endswith(".txt"):
        download_filename = get_unique_filename(
            directory_out / create_file_list, overwrite
        )
        logger.info(f"The file list is written at {download_filename}")
        with open(download_filename, "w") as file_out:
            for filename in filenames:
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
                size_file,
                last_modified_datetime,
                etag,
            ) in zip(filenames, sizes, last_modified_datetimes, etags):
                file_out.write(
                    f"{filename},{size_file},{last_modified_datetime},{etag}\n"
                )
        return None
    locator = (endpoint_url, bucket)
    return (
        locator,
        filenames,
        sizes,
        last_modified_datetimes,
        total_size,
        filenames_without_sync,
    )


def _download_header_for_direct_download(
    files_to_download: list[str],
    dataset_url: str,
    sync: bool,
    directory_out: pathlib.Path,
    username: str,
) -> Tuple[
    Tuple[str, str],
    List[str],
    List[float],
    List[datetime.datetime],
    float,
    list[str],
    list[str],
]:
    (endpoint_url, bucket, path) = parse_access_dataset_url(dataset_url)
    splitted_path = path.split("/")
    root_folder = splitted_path[0]
    product_id = splitted_path[1]
    dataset_id_with_tag = splitted_path[2]

    sizes = []
    last_modified_datetimes = []
    filenames_in = []
    filenames_without_sync = []
    filenames_not_found = []
    for file_to_download in files_to_download:
        file_path = file_to_download.split(f"{dataset_id_with_tag}/")[-1]
        if not file_path:
            logger.warning(
                f"{file_to_download} does not seem to be valid. Skipping."
            )
            filenames_not_found.append(file_path)
            continue
        full_path = (
            f"s3://{bucket}/{root_folder}/{product_id}/"
            f"{dataset_id_with_tag}/{file_path}"
        )
        size_and_last_modified = _get_file_size_and_last_modified(
            endpoint_url, bucket, full_path, username
        )
        if size_and_last_modified:
            size, last_modified = size_and_last_modified
            if not sync or _check_needs_to_be_synced(
                full_path, size, last_modified, directory_out
            ):
                filenames_in.append(full_path)
                sizes.append(float(size))
                last_modified_datetimes.append(last_modified)
            else:
                filenames_without_sync.append(full_path)
        else:
            filenames_not_found.append(file_path)
    if not filenames_in:
        logger.warning(
            "No files found to download for direct download. "
            "Please check the files to download. "
            "We will try to list the files available for download "
            "and compare them with the requested files."
        )
    total_size = sum([size for size in sizes])
    return (
        (endpoint_url, bucket),
        filenames_in,
        sizes,
        last_modified_datetimes,
        total_size,
        filenames_without_sync,
        filenames_not_found,
    )


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


def _create_information_message_before_download(
    filenames: list[str],
    sizes: list[float],
    last_modified_datetimes: list[datetime.datetime],
    total_size: float,
) -> str:
    message = "You requested the download of the following files:\n"
    for filename, size, last_modified_datetime in zip(
        filenames[:20], sizes[:20], last_modified_datetimes[:20]
    ):
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
    return message


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

    s3_client, _ = get_configured_boto3_session(
        endpoint_url, ["ListObjects"], username
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


def _get_file_size_and_last_modified(
    endpoint_url: str, bucket: str, file_in: str, username: str
) -> Optional[Tuple[int, datetime.datetime]]:
    s3_client, _ = get_configured_boto3_session(
        endpoint_url, ["HeadObject"], username
    )

    try:
        s3_object = s3_client.head_object(
            Bucket=bucket,
            Key=file_in.replace(f"s3://{bucket}/", ""),
        )
        return s3_object["ContentLength"], s3_object["LastModified"]
    except ClientError as e:
        if "404" in str(e):
            logger.warning(
                f"File {file_in} not found on the server. Skipping."
            )
            return None
        else:
            raise e


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

    def _original_files_file_download(
        endpoint_url: str, bucket: str, file_in: str, file_out: pathlib.Path
    ) -> pathlib.Path:
        """
        Download ONE file and return a string of the result
        """
        s3_client, s3_resource = get_configured_boto3_session(
            endpoint_url,
            ["GetObject", "HeadObject"],
            username,
            return_ressources=True,
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
