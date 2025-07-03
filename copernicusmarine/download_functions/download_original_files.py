import logging
import os
import pathlib
import re
from datetime import datetime
from itertools import chain
from typing import Literal, Optional

from botocore.client import ClientError
from dateutil.tz import UTC
from tqdm import tqdm

from copernicusmarine.core_functions.models import (
    FileGet,
    FileStatus,
    ResponseGet,
    S3FileInfo,
    S3FilesDescriptor,
    StatusCode,
    StatusMessage,
)
from copernicusmarine.core_functions.request_structure import (
    GetRequest,
    overload_regex_with_additionnal_filter,
)
from copernicusmarine.core_functions.sessions import (
    get_configured_boto3_session,
)
from copernicusmarine.core_functions.utils import (
    get_unique_filepath,
    parse_access_dataset_url,
    run_concurrently,
    timestamp_parser,
)

logger = logging.getLogger("copernicusmarine")


def download_original_files(
    username: str,
    password: str,
    get_request: GetRequest,
    max_concurrent_requests: int,
    disable_progress_bar: bool,
    create_file_list: Optional[str],
) -> ResponseGet:
    endpoint, bucket, path = parse_access_dataset_url(
        str(get_request.dataset_url)
    )
    if get_request.direct_download:
        files_headers = _download_header_for_direct_download(
            files_to_download=get_request.direct_download,
            endpoint_url=endpoint,
            bucket=bucket,
            path=path,
            sync=get_request.sync,
            directory_out=pathlib.Path(get_request.output_directory),
            username=username,
            no_directories=get_request.no_directories,
            overwrite=get_request.overwrite,
            skip_existing=get_request.skip_existing,
        )
    else:
        files_headers = S3FilesDescriptor(endpoint=endpoint, bucket=bucket)
    if (
        not get_request.direct_download
        or files_headers.files_not_found
        or get_request.regex
    ):
        if files_headers.files_not_found:
            files_not_found_regex = "|".join(
                [
                    re.escape(file_not_found)
                    for file_not_found in files_headers.files_not_found
                ]
            )
            get_request.regex = overload_regex_with_additionnal_filter(
                files_not_found_regex, get_request.regex
            )
        if get_request.index_parts:
            _, _, path = parse_access_dataset_url(
                str(get_request.dataset_url), only_dataset_root_path=True
            )
        files_headers_listing = _download_header(
            endpoint_url=endpoint,
            bucket=bucket,
            path=path,
            regex=get_request.regex,
            username=username,
            sync=get_request.sync,
            create_file_list=create_file_list,
            directory_out=pathlib.Path(get_request.output_directory),
            no_directories=get_request.no_directories,
            skip_existing=get_request.skip_existing,
            overwrite=get_request.overwrite,
            disable_progress_bar=disable_progress_bar,
            only_list_root_path=get_request.index_parts,
        )
        if files_headers_listing.create_file_list is True:
            return ResponseGet(
                files=[],
                files_deleted=None,
                files_not_found=None,
                number_of_files_to_download=0,
                status=StatusCode.FILE_LIST_CREATED,
                message=StatusMessage.FILE_LIST_CREATED,
                total_size=None,
            )
        if files_headers_listing:
            files_headers.endpoint = files_headers_listing.endpoint
            files_headers.bucket = files_headers_listing.bucket
            files_headers.s3_files.extend(files_headers_listing.s3_files)
            files_headers.total_size += files_headers_listing.total_size
            files_headers.files_not_found.extend(
                files_headers_listing.files_not_found
            )

    files_headers = _create_filenames_out(
        files_information=files_headers,
        output_directory=pathlib.Path(get_request.output_directory),
        no_directories=get_request.no_directories,
    )

    if get_request.sync_delete:
        files_headers = _get_files_to_delete_with_sync(
            files_information=files_headers,
            output_directory=pathlib.Path(get_request.output_directory),
        )
        if files_headers.files_to_delete:
            logger.info("Some files will be deleted due to sync delete:")
            for file_to_delete in files_headers.files_to_delete:
                logger.info(file_to_delete)
                file_to_delete.unlink()
    if files_headers.total_size == 0:
        logger.info("No data to download")
        if not files_headers.files_to_delete:
            return create_response_get_from_files_headers(
                files_headers, get_request, "NO_DATA_TO_DOWNLOAD"
            )

    response = create_response_get_from_files_headers(
        files_headers, get_request, "SUCCESS"
    )

    if get_request.dry_run:
        response.status = StatusCode.DRY_RUN
        response.message = StatusMessage.DRY_RUN
        return response
    filenames_in = []
    filenames_out = []
    for s3_file in files_headers.s3_files:
        if not s3_file.ignore:
            filenames_in.append(s3_file.filename_in)
            filenames_out.append(s3_file.filename_out)
    download_files(
        username,
        endpoint,
        bucket,
        filenames_in,
        filenames_out,
        max_concurrent_requests,
        disable_progress_bar,
    )
    return response


def create_response_get_from_files_headers(
    files_headers: S3FilesDescriptor,
    get_request: GetRequest,
    status: Literal["SUCCESS", "DRY_RUN", "NO_DATA_TO_DOWNLOAD"],
) -> ResponseGet:
    endpoint = files_headers.endpoint
    return ResponseGet(
        files=[
            FileGet(
                s3_url=s3_file.filename_in,
                https_url=s3_file.filename_in.replace("s3://", endpoint + "/"),
                file_size=size_to_MB(s3_file.size),
                last_modified_datetime=s3_file.last_modified,
                etag=s3_file.etag,
                output_directory=pathlib.Path(get_request.output_directory),
                filename=s3_file.filename_out.name,
                file_path=s3_file.filename_out,
                file_format=s3_file.filename_out.suffix,
                file_status=FileStatus.get_status(
                    ignore=s3_file.ignore, overwrite=s3_file.overwrite
                ),
            )
            for s3_file in files_headers.s3_files
        ],
        files_deleted=(
            [
                str(file_to_delete)
                for file_to_delete in files_headers.files_to_delete
            ]
            if files_headers.files_to_delete
            else None
        ),
        files_not_found=(
            files_headers.files_not_found
            if files_headers.files_not_found
            else None
        ),
        number_of_files_to_download=len(
            [
                file_to_download
                for file_to_download in files_headers.s3_files
                if not file_to_download.ignore
            ]
        ),
        status=(
            StatusCode.NO_DATA_TO_DOWNLOAD
            if status == "NO_DATA_TO_DOWNLOAD"
            else (
                StatusCode.DRY_RUN
                if status == "DRY_RUN"
                else StatusCode.SUCCESS
            )
        ),
        message=(
            StatusMessage.NO_DATA_TO_DOWNLOAD
            if status == "NO_DATA_TO_DOWNLOAD"
            else (
                StatusMessage.DRY_RUN
                if status == "DRY_RUN"
                else StatusMessage.SUCCESS
            )
        ),
        total_size=size_to_MB(files_headers.total_size),
    )


def _get_files_to_delete_with_sync(
    files_information: S3FilesDescriptor,
    output_directory: pathlib.Path,
) -> S3FilesDescriptor:
    if not files_information.s3_files:
        return files_information
    product_structure = _local_path_from_s3_url(
        files_information.s3_files[0].filename_in, pathlib.Path("")
    ).parts
    product_id = product_structure[0]
    dataset_id = product_structure[1]
    dataset_level_local_folder = output_directory / product_id / dataset_id
    filenames_out = {
        s3_file.filename_out for s3_file in files_information.s3_files
    }
    for local_file in dataset_level_local_folder.glob("**/*"):
        if local_file.is_file() and local_file not in filenames_out:
            files_information.files_to_delete.append(local_file)
    return files_information


def download_files(
    username: str,
    endpoint_url: str,
    bucket: str,
    filenames_in: list[str],
    filenames_out: list[pathlib.Path],
    max_concurrent_requests: int,
    disable_progress_bar: bool,
) -> None:
    for filename_out in filenames_out:
        parent_dir = pathlib.Path(filename_out).parent
        if not parent_dir.is_dir():
            pathlib.Path.mkdir(parent_dir, parents=True)
    if max_concurrent_requests:
        run_concurrently(
            _download_one_file,
            [
                (username, endpoint_url, bucket, in_file, str(out_file))
                for in_file, out_file in zip(
                    filenames_in,
                    filenames_out,
                )
            ],
            max_concurrent_requests,
            tdqm_bar_configuration={
                "disable": disable_progress_bar,
                "desc": "Downloading files",
            },
        )
    else:
        logger.info("Downloading files one by one...")
        with tqdm(
            total=len(filenames_in),
            disable=disable_progress_bar,
            desc="Downloading files",
        ) as pbar:
            for in_file, out_file in zip(filenames_in, filenames_out):
                _download_one_file(
                    username, endpoint_url, bucket, in_file, str(out_file)
                )
                pbar.update(1)


def _download_header(
    endpoint_url: str,
    bucket: str,
    path: str,
    regex: Optional[str],
    username: str,
    sync: bool,
    create_file_list: Optional[str],
    directory_out: pathlib.Path,
    no_directories: bool,
    overwrite: bool,
    skip_existing: bool,
    disable_progress_bar: bool,
    only_list_root_path: bool = False,
) -> S3FilesDescriptor:

    files_headers = S3FilesDescriptor(endpoint=endpoint_url, bucket=bucket)

    raw_filenames = _list_files_on_marine_data_lake_s3(
        username,
        endpoint_url,
        bucket,
        path,
        not only_list_root_path,
        disable_progress_bar,
    )

    for filename, size, last_modified_datetime, etag in raw_filenames:
        if not regex or re.search(regex, filename):
            file_to_append = S3FileInfo(
                filename_in=filename,
                size=float(size),
                last_modified=last_modified_datetime.isoformat(),
                etag=etag,
                ignore=_check_should_be_ignored(
                    filename,
                    size,
                    last_modified_datetime,
                    directory_out,
                    skip_existing,
                    sync,
                    no_directories,
                ),
                overwrite=_check_should_be_overwritten(
                    filename,
                    size,
                    last_modified_datetime,
                    directory_out,
                    sync,
                    overwrite,
                    no_directories,
                ),
            )
            files_headers.add_s3_file(file_to_append)

    if create_file_list and create_file_list.endswith(".txt"):
        download_filename = directory_out / create_file_list
        if not overwrite:
            download_filename = get_unique_filepath(
                directory_out / create_file_list,
            )
        with open(download_filename, "w") as file_out:
            for s3_file in files_headers.s3_files:
                if not s3_file.ignore:
                    file_out.write(f"{s3_file.filename_in}\n")
        files_headers.create_file_list = True
    elif create_file_list and create_file_list.endswith(".csv"):
        download_filename = directory_out / create_file_list
        if not overwrite:
            download_filename = get_unique_filepath(
                directory_out / create_file_list,
            )
        with open(download_filename, "w") as file_out:
            file_out.write("filename,size,last_modified_datetime,etag\n")
            for s3_file in files_headers.s3_files:
                if not s3_file.ignore:
                    file_out.write(
                        f"{s3_file.filename_in},{s3_file.size},"
                        f"{s3_file.last_modified},{s3_file.etag}\n"
                    )
        files_headers.create_file_list = True
    return files_headers


def _download_header_for_direct_download(
    files_to_download: list[str],
    endpoint_url: str,
    bucket: str,
    path: str,
    sync: bool,
    directory_out: pathlib.Path,
    username: str,
    no_directories: bool,
    overwrite: bool,
    skip_existing: bool,
) -> S3FilesDescriptor:

    files_headers = S3FilesDescriptor(endpoint=endpoint_url, bucket=bucket)

    split_path = path.split("/")
    root_folder = split_path[0]
    product_id = split_path[1]
    dataset_id_with_tag = split_path[2]

    for file_to_download in files_to_download:
        file_path = file_to_download.split(f"{dataset_id_with_tag}/")[-1]
        if not file_path:
            logger.warning(
                f"{file_to_download} does not seem to be valid. Skipping."
            )
            files_headers.files_not_found.append(file_to_download)
            continue
        full_path = (
            f"s3://{bucket}/{root_folder}/{product_id}/"
            f"{dataset_id_with_tag}/{file_path}"
        )
        size_last_modified_and_etag = _get_file_size_last_modified_and_etag(
            endpoint_url, bucket, full_path, username
        )
        if size_last_modified_and_etag:
            size, last_modified, etag = size_last_modified_and_etag
            file_to_append = S3FileInfo(
                filename_in=full_path,
                size=size,
                last_modified=last_modified.isoformat(),
                etag=etag,
                ignore=_check_should_be_ignored(
                    full_path,
                    size,
                    last_modified,
                    directory_out,
                    skip_existing,
                    sync,
                    no_directories,
                ),
                overwrite=_check_should_be_overwritten(
                    full_path,
                    size,
                    last_modified,
                    directory_out,
                    sync,
                    overwrite,
                    no_directories,
                ),
            )
            files_headers.add_s3_file(file_to_append)
        else:
            files_headers.files_not_found.append(file_to_download)

    if not files_headers.s3_files:
        logger.warning(
            "No files found to download for direct download. "
            "Please check the files to download. "
            "We will try to list the files available for download "
            "and compare them with the requested files."
        )

    return files_headers


def _check_already_exists(
    filename: str,
    directory_out: pathlib.Path,
    no_directories: bool,
) -> bool:
    filename_out = _create_filename_out(
        filename, directory_out, no_directories
    )
    return filename_out.is_file()


def _check_needs_to_be_synced(
    filename: str,
    size: int,
    last_modified_datetime: datetime,
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
            last_created_datetime_out = timestamp_parser(
                file_stats.st_mtime, unit="s"
            )
            # boto3.s3_resource.Object.last_modified is without microsecond
            # boto3.paginate s3_object["LastModified"] is with microsecond
            last_modified_datetime = last_modified_datetime.replace(
                microsecond=0
            )
            return last_modified_datetime > last_created_datetime_out


def _check_should_be_ignored(
    filename: str,
    size: int,
    last_modified_datetime: datetime,
    directory_out: pathlib.Path,
    skip_existing: bool,
    sync: bool,
    no_directories: bool,
) -> bool:
    return (
        skip_existing
        and _check_already_exists(filename, directory_out, no_directories)
    ) or (
        sync
        and not _check_needs_to_be_synced(
            filename, size, last_modified_datetime, directory_out
        )
    )


def _check_should_be_overwritten(
    filename: str,
    size: int,
    last_modified_datetime: datetime,
    directory_out: pathlib.Path,
    sync: bool,
    overwrite: bool,
    no_directories: bool,
) -> bool:
    return (
        overwrite
        and _check_already_exists(filename, directory_out, no_directories)
        or (
            sync
            and _check_needs_to_be_synced(
                filename, size, last_modified_datetime, directory_out
            )
        )
    )


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
    disable_progress_bar: bool,
) -> list[tuple[str, int, datetime, str]]:
    s3_client, _ = get_configured_boto3_session(
        endpoint_url, ["ListObjectsV2"], username
    )

    paginator = s3_client.get_paginator("list_objects")
    page_iterator = paginator.paginate(
        Bucket=bucket,
        Prefix=prefix,
        Delimiter="/" if not recursive else "",
    )
    logger.info("Listing files on remote server...")
    s3_objects = chain(
        *map(
            lambda page: page.get("Contents", []),
            tqdm(page_iterator, disable=disable_progress_bar),
        )
    )
    files_already_found: list[tuple[str, int, datetime, str]] = []
    for s3_object in s3_objects:
        files_already_found.append(
            (
                f"s3://{bucket}/" + s3_object["Key"],
                s3_object["Size"],
                s3_object["LastModified"].astimezone(tz=UTC),
                s3_object["ETag"],
            )
        )
    return files_already_found


def _get_file_size_last_modified_and_etag(
    endpoint_url: str, bucket: str, file_in: str, username: str
) -> Optional[tuple[int, datetime, str]]:
    s3_client, _ = get_configured_boto3_session(
        endpoint_url, ["HeadObject"], username
    )

    try:
        s3_object = s3_client.head_object(
            Bucket=bucket,
            Key=file_in.replace(f"s3://{bucket}/", ""),
        )
        return (
            s3_object["ContentLength"],
            s3_object["LastModified"].astimezone(tz=UTC),
            s3_object["ETag"],
        )
    except ClientError as e:
        if "404" in str(e):
            logger.warning(
                f"File {file_in} not found on the server. Skipping."
            )
            return None
        else:
            raise e


def _download_one_file(
    username,
    endpoint_url: str,
    bucket: str,
    file_in: str,
    file_out: str,
) -> None:
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

    try:
        os.utime(
            file_out, (last_modified_date_epoch, last_modified_date_epoch)
        )
    except PermissionError:
        logger.warning(
            f"Permission to modify the last modified date "
            f"of the file {file_out} is denied."
        )


# /////////////////////////////
# --- Tools
# /////////////////////////////


def _create_filenames_out(
    files_information: S3FilesDescriptor,
    output_directory: pathlib.Path = pathlib.Path("."),
    no_directories=False,
) -> S3FilesDescriptor:
    for s3_file in files_information.s3_files:
        filename_in = s3_file.filename_in
        filename_out = _create_filename_out(
            filename_in,
            output_directory,
            no_directories,
        )
        if not s3_file.overwrite and not s3_file.ignore:
            filename_out = get_unique_filepath(
                filepath=filename_out,
            )
        s3_file.filename_out = filename_out

    return files_information


def _create_filename_out(
    file_path: str,
    output_directory: pathlib.Path = pathlib.Path("."),
    no_directories=False,
):
    if no_directories:
        filename_out = (
            pathlib.Path(output_directory) / pathlib.Path(file_path).name
        )
    else:
        # filename_in: s3://mdl-native-xx/native/<product-id>..
        filename_out = _local_path_from_s3_url(file_path, output_directory)
    return filename_out


def size_to_MB(size: float) -> float:
    return size / 1024**2
