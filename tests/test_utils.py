import logging
import os
import pathlib
import platform
import subprocess

# import sys
import time
from subprocess import CompletedProcess
from typing import Optional

logger = logging.getLogger()


class FileToCheck:
    def __init__(self, file_path: str):
        self.file_path = file_path

    def get_path(self) -> str:
        if platform.system() == "Windows":
            # Convert to Windows path format if necessary
            return self.file_path.replace("/", "\\")
        return self.file_path


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


# def get_poetry_python() -> str:
#     """Get the Python executable from the Poetry virtual environment"""
#     try:
#         result = subprocess.run(
#             ["poetry", "env", "info", "--path"],
#             capture_output=True,
#             text=True,
#             check=True,
#         )
#         venv_path = result.stdout.strip()
#         return os.path.join(venv_path, "Scripts", "python.exe")
#     except subprocess.CalledProcessError:
#         return sys.executable


def execute_in_terminal(
    command: list[str],
    timeout_second: float = FIVE_MINUTES,
    user_input: Optional[str] = None,
    env: Optional[dict[str, str]] = None,
    shell: Optional[bool] = None,
) -> CompletedProcess[str]:
    t1 = time.time()
    command_to_print = " ".join([str(c) for c in command])
    logger.info(f"Running command: {command_to_print}...")
    if platform.system() == "Windows" and shell is None:
        shell = True
    elif platform.system() == "Windows" and shell is False:
        # Get Poetry environment path
        result = subprocess.run(
            ["poetry", "env", "info", "--path"], capture_output=True, text=True
        )
        venv_path = result.stdout.strip()
        print(f"Virtual env path: {venv_path}")

        # Check what's in the Scripts directory
        scripts_dir = os.path.join(venv_path, "Scripts")
        print(f"Scripts directory: {scripts_dir}")
        print(f"Scripts directory exists: {os.path.exists(scripts_dir)}")

        if os.path.exists(scripts_dir):
            print("Contents of Scripts directory:")
            for item in os.listdir(scripts_dir):
                if "copernicus" in item.lower():
                    full_path = os.path.join(scripts_dir, item)
                    print(f"  {item} -> {full_path}")
                    print(f"    Exists: {os.path.exists(full_path)}")
                    print(f"    Is file: {os.path.isfile(full_path)}")

        # Set up environment like Poetry would
        env = os.environ.copy()
        env["PATH"] = (
            os.path.join(venv_path, "Scripts") + os.pathsep + env["PATH"]
        )
        command[0] = os.path.join(scripts_dir, "copernicusmarine")
    else:
        shell = False
    output = subprocess.run(
        command,
        capture_output=True,
        timeout=timeout_second,
        input=user_input,
        env=env,
        text=True,
        shell=shell,
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
