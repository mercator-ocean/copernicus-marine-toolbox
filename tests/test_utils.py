import logging
import os
import pathlib
import platform
import subprocess
import time
from subprocess import CompletedProcess

logger = logging.getLogger()


class FileToCheck:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def get_path(self) -> str:
        if platform.system() == "Windows":
            return self.file_path.replace("/", "\\")
        return self.file_path


def get_file_size(filepath):
    file_path = pathlib.Path(filepath)
    file_stats = file_path.stat()
    return file_stats.st_size


def get_all_files_in_folder_tree(folder: str) -> list[str]:
    downloaded_files = []
    for _, _, files in os.walk(folder):
        for filename in files:
            downloaded_files.append(filename)
    return downloaded_files


def _remove_logging_prefix(full_message: str) -> str:
    return full_message.split(" - ", 2)[2]


def remove_extra_logging_prefix_info(multi_line_message: str) -> str:
    if platform.system() == "Windows":
        multi_line_message = multi_line_message.rstrip("\r\n")
        return "\n".join(
            map(_remove_logging_prefix, multi_line_message.split("\r\n"))
        )
    multi_line_message = multi_line_message.rstrip("\n")
    return "\n".join(
        map(_remove_logging_prefix, multi_line_message.split("\n"))
    )


FIVE_MINUTES = 5 * 60


def execute_in_terminal(
    command: list[str],
    timeout_second: float = FIVE_MINUTES,
    user_input: str | None = None,
    env: dict[str, str] | None = None,
    shell: bool = True,
    safe_quoting: bool = False,
) -> CompletedProcess[str]:
    t1 = time.time()
    command_to_print = " ".join([str(c) for c in command])
    logger.info(f"Running command: {command_to_print}...")
    if platform.system() == "Windows" and not safe_quoting:

        output = subprocess.run(
            command,
            capture_output=True,
            timeout=timeout_second,
            input=user_input,
            env=env,
            text=True,
            encoding="utf-8",
            shell=shell,
            errors="replace",
        )
    elif platform.system() == "Windows" and safe_quoting:

        def windows_quote(arg):
            arg_str = str(arg)
            special_chars = ' "&<>|^()!%/'
            if any(char in arg_str for char in special_chars):
                escaped = arg_str.replace('"', '""')
                return f'"{escaped}"'
            return arg_str

        command_str = " ".join(windows_quote(arg) for arg in command)
        logger.info(f"Running command with quoting: {command_str}...")
        output = subprocess.run(
            command_str,
            capture_output=True,
            timeout=timeout_second,
            input=user_input,
            env=env,
            text=True,
            shell=shell,
            encoding="utf-8",
            errors="replace",
        )
    else:
        output = subprocess.run(
            command,
            capture_output=True,
            timeout=timeout_second,
            input=user_input,
            env=env,
            text=True,
        )
    t2 = time.time()
    duration_second = t2 - t1
    logger.info(f"Command executed in {duration_second} s: {command_to_print}")
    return output


def main_checks_when_file_is_downloaded(
    file_path: pathlib.Path,
    response: dict,
):
    size_variance = 0.2
    offset_size = 0.05  # small datasets are hard to predict
    file_size = os.path.getsize(file_path) / 1048e3  # in MB

    if file_path.suffix == ".csv":
        assert (
            file_size
            <= response["file_size"] * (1 + size_variance) + offset_size
        )
        assert (
            file_size
            >= response["file_size"] * (1 - size_variance) - offset_size
        )
        return

    if file_path.suffix == ".nc":
        assert (
            file_size
            <= response["file_size"] * (1 + size_variance) + offset_size
        )
        assert (
            file_size
            >= response["file_size"] * (1 - size_variance) - offset_size
        )
    assert response["file_size"] <= response["data_transfer_size"]
