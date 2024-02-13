from functools import wraps
from typing import Any, Callable, TypeVar

import click

T = TypeVar("T", bound=Callable[..., Any])


def log_exception_and_exit(function: T) -> T:
    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except click.Abort:
            print("Abort")
        except Exception as exception:
            raise exception

    return wrapper  # type: ignore
