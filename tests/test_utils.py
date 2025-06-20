import logging
import os
import pathlib
import platform
import subprocess
import time
from subprocess import CompletedProcess
from typing import Optional

logger = logging.getLogger()


def _remove_loggin_prefix(full_message: str) -> str:
    return full_message.split(" - ", 2)[2]


def remove_extra_logging_prefix_info(multi_line_message: str) -> str:
    if platform.system() == "Windows":
        multi_line_message = multi_line_message.rstrip("\r\n")
        return "\n".join(
            map(_remove_loggin_prefix, multi_line_message.split("\r\n"))
        )
    multi_line_message = multi_line_message.rstrip("\n")
    return "\n".join(
        map(_remove_loggin_prefix, multi_line_message.split("\n"))
    )


FIVE_MINUTES = 5 * 60


def execute_in_terminal(
    command: list[str],
    timeout_second: float = FIVE_MINUTES,
    user_input: Optional[str] = None,
    env: Optional[dict[str, str]] = None,
) -> CompletedProcess[str]:
    t1 = time.time()
    command_to_print = " ".join([str(c) for c in command])
    logger.info(f"Running command: {command_to_print}...")
    # import platform

    # if platform.system() == "Windows":
    #     # On Windows, try to resolve the executable with .exe extension
    #     import shutil

    #     executable = shutil.which(command[0])
    #     if executable:
    #         command = [executable] + command[1:]
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
    file_size = os.path.getsize(file_path)
    if file_path.suffix == ".nc":
        assert (
            file_size / 1048e3
            <= response["file_size"] * (1 + size_variance) + offset_size
        )
        assert (
            file_size / 1048e3
            >= response["file_size"] * (1 - size_variance) - offset_size
        )
    assert response["file_size"] <= response["data_transfer_size"]
    return
