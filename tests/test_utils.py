import logging
import subprocess
import time
from subprocess import CompletedProcess

logger = logging.getLogger()


def _remove_loggin_prefix(full_message: bytes):
    return full_message.split(b" - ", 2)[2]


def remove_extra_logging_prefix_info(multi_line_message: bytes):
    multi_line_message = multi_line_message.rstrip(b"\n")
    return b"\n".join(
        map(_remove_loggin_prefix, multi_line_message.split(b"\n"))
    )


FIVE_MINUTES = 5 * 60


def execute_in_terminal(
    command: list[str],
    timeout_second: float = FIVE_MINUTES,
    input=None,
    env=None,
) -> CompletedProcess[bytes]:
    t1 = time.time()
    logger.info(f"Running command: {' '.join(command)}...")
    output = subprocess.run(
        command,
        capture_output=True,
        timeout=timeout_second,
        input=input,
        env=env,
    )
    t2 = time.time()
    duration_second = t2 - t1
    logger.info(
        f"Command executed in {duration_second} s: {' '.join(command)}"
    )
    return output
