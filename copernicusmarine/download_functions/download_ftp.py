import logging
import os
import pathlib
import re
from datetime import datetime
from ftplib import FTP
from itertools import chain
from multiprocessing.pool import ThreadPool
from typing import Any, List, Optional, Tuple
from urllib.parse import urlparse

from numpy import append, arange
from tqdm import tqdm

from copernicusmarine.catalogue_parser.request_structure import GetRequest
from copernicusmarine.core_functions.utils import flatten, get_unique_filename
from copernicusmarine.download_functions.download_get import download_get

# /////////////////////////////
# ---Using ftplib
# /////////////////////////////

logger = logging.getLogger("copernicus_marine_root_logger")


class FTPConnection:
    def __init__(self, host: str, username: str, password: str) -> None:
        if ftp_proxy := os.environ.get("FTP_PROXY", None):
            url_obj = urlparse(ftp_proxy)
            if (
                url_obj.port is None
                or url_obj.hostname is None
                or url_obj.password is None
                or url_obj.username is None
            ):
                raise ValueError(
                    "FTP_PROXY must contain a port number, "
                    "a hostname, a username and a password"
                )
            host_proxy = url_obj.hostname
            port_proxy = url_obj.port
            user_proxy = url_obj.username
            password_proxy = url_obj.password
            ftp = FTP()
            ftp.connect(host_proxy, port_proxy)
            ftp_loginstring = f"{username}@{host} {user_proxy}"
            ftp.login(
                user=ftp_loginstring,
                passwd=password,
                acct=password_proxy,
            )
            self.ftp = ftp
        else:
            self.ftp = FTP(host, user=username, passwd=password)

    def __enter__(self):
        return self.ftp

    def __exit__(self, exc_type, exc_value, traceback):
        self.ftp.quit()


def download_ftp(
    username: str,
    password: str,
    get_request: GetRequest,
    disable_progress_bar: bool,
    download_file_list: bool,
) -> list[pathlib.Path]:
    logger.warning(
        "The FTP service is deprecated, please use 'original-files' instead."
    )
    filenames_in, filenames_out, host = download_get(
        username,
        password,
        get_request,
        download_file_list,
        download_header,
        create_filenames_out,
    )
    return download_files(
        host,
        username,
        password,
        filenames_in,
        filenames_out,
        disable_progress_bar,
    )


def download_files(
    host: str,
    username: str,
    password: str,
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
    groups_in_files_count = len(groups_in_files)
    download_summary_list = pool.imap(
        _download_files,
        zip(
            [host] * groups_in_files_count,
            [username] * groups_in_files_count,
            [password] * groups_in_files_count,
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


def download_header(
    data_path: str,
    regex: Optional[str],
    username: str,
    password: str,
    output_directory: pathlib.Path,
    download_file_list: bool,
) -> Tuple[str, str, list[str], float]:
    (host, path) = parse_ftp_dataset_url(data_path)
    logger.debug(f"Downloading header via FTP on {host + path}")
    message = "You requested the download of the following files:\n"
    total_size = 0
    with FTPConnection(host, username, password) as ftp:
        raw_filenames = get_filenames_recursively(ftp, path)
        if regex:
            regex_compiled = re.compile(regex)
            filenames = list(filter(regex_compiled.search, raw_filenames))
        else:
            filenames = raw_filenames

    if download_file_list:
        download_filename = get_unique_filename(
            output_directory / "files_to_download.txt", False
        )
        logger.info(f"The file list is written at {download_filename}")
        with open(download_filename, "w") as file_out:
            for filename in filenames:
                file_out.write(f"{filename}\n")
        exit(0)

    pool = ThreadPool()
    nfilenames_per_process, nfilenames = 100, len(filenames)
    indexes = append(
        arange(0, nfilenames, nfilenames_per_process, dtype=int),
        nfilenames,
    )
    groups_filenames = [
        filenames[indexes[i] : indexes[i + 1]] for i in range(len(indexes) - 1)
    ]
    results_size = pool.map(
        get_filename_size_tuple,
        zip(
            [host] * len(groups_filenames),
            [username] * len(groups_filenames),
            [password] * len(groups_filenames),
            groups_filenames,
        ),
    )
    flattened_results_size = list(chain(*results_size))

    total_size += sum([int(res[1]) for res in flattened_results_size])
    for result in flattened_results_size[:20]:
        message += str(result[0])
        message += f" - {format_file_size(float(result[1]))}"
        message += f" - {result[2]}\n"

    if len(flattened_results_size) > 20:
        message += f"Printed 20 out of {len(flattened_results_size)} files\n"
    message += (
        f"\nTotal size of the download: {format_file_size(total_size)}\n\n"
    )
    return (message, host, filenames, total_size)


def get_filenames_recursively(
    ftp: FTP, path: str, extensions: list[str] = [".nc"]
) -> list[str]:
    logger.debug(f"Downloading header via FTP on {path}")
    if any(extension in path for extension in extensions):
        # path is a file
        return [path]
    elif len(ftp.nlst(path)) == 0:
        # empty dir
        return []
    elif ftp.nlst(path)[0] == path:
        # path is a file
        return [path]
    else:
        # path is a dir
        return [
            filename
            for element in ftp.nlst(path)
            for filename in get_filenames_recursively(ftp, element)
        ]


def get_filename_size_tuple(
    tuple_ftp_filename: Tuple[str, str, str, list[str]]
) -> list[Tuple[str, Any, str]]:
    host, username, password, filenames = tuple_ftp_filename
    with FTPConnection(host, username, password) as ftp:
        list_tuples = [
            (
                filename,
                ftp.size(filename),
                get_last_modified_datetime(ftp, filename),
            )
            for filename in filenames
        ]
    return list_tuples


def get_last_modified_datetime(ftp: FTP, filename: str) -> str:
    result_code_and_datetime = ftp.voidcmd("MDTM " + filename)
    result_code = result_code_and_datetime.split(" ")[0]
    if result_code != "213":
        return "N/C"
    ftp_datetime = result_code_and_datetime.split(" ")[1]
    datetime_iso = datetime.strptime(ftp_datetime, "%Y%m%d%H%M%S").strftime(
        "%Y-%m-%dT%H:%M:%S:%SZ"
    )
    return datetime_iso


def _download_files(
    tuple_ftp_filename: Tuple[str, str, str, list[str], list[pathlib.Path]],
) -> list[pathlib.Path]:
    def _ftp_file_download(ftp: FTP, file_in: str, file_out: pathlib.Path):
        """
        Download ONE file and return a string of the result
        """
        pathlib.Path.mkdir(file_out.parent, parents=True, exist_ok=True)
        with open(file_out, "wb") as fp:

            def callback(data):
                fp.write(data)

            res = ftp.retrbinary(f"RETR {file_in}", callback)

            if not res.startswith("226 Transfer complete"):
                logger.error(f"Download {file_in} failed")
                if file_out.is_file():
                    file_out.unlink()
                summary_string = file_out
            else:
                summary_string = file_out
        return summary_string

    host, username, password, filenames_in, filenames_out = tuple_ftp_filename
    download_summary: list[pathlib.Path] = []
    with FTPConnection(host, username, password) as ftp:
        for file_in, file_out in zip(filenames_in, filenames_out):
            download_summary.append(_ftp_file_download(ftp, file_in, file_out))
    return download_summary


# /////////////////////////////
# --- Tools
# /////////////////////////////


def parse_ftp_dataset_url(data_path: str) -> tuple[str, str]:
    host = data_path[len("ftp://") :].split("/")[0]
    path = data_path[len("ftp://" + host + "/") :]
    return (host, path)


def create_filenames_out(
    filenames_in: list[str],
    overwrite: bool,
    output_directory: pathlib.Path = pathlib.Path("."),
    no_directories=False,
) -> list[pathlib.Path]:
    filenames_out = []
    for filename_in in filenames_in:
        filename_out = output_directory
        if no_directories:
            filename_out = (
                pathlib.Path(filename_out) / pathlib.Path(filename_in).name
            )
        elif filename_in.startswith("Core/"):
            filename_out = filename_out / pathlib.Path(
                filename_in[len("Core/") :]
            )

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
