import logging
from functools import wraps
from itertools import chain
from re import finditer
from sys import exit
from typing import Callable

logger = logging.getLogger("copernicusmarine")


def _log_exception(log_function: Callable, exception: Exception):
    exception_string = str(exception).strip('"')
    details = f": {exception_string}" if exception_string else ""
    message = _exception_to_sentence(exception) + details
    log_function(message)


def log_exception_debug(exception: Exception):
    _log_exception(logger.debug, exception)


def log_exception_and_exit(function: Callable):
    @wraps(function)
    def wrapper(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except Exception as exception:
            try:
                custom_exception_message = exception.__getattribute__(
                    "custom_exception_message"
                )
                logger.error(custom_exception_message)
            except AttributeError:
                _log_exception(logger.error, exception)
            exit(1)

    return wrapper


def _exception_to_sentence(exception: Exception) -> str:
    return _camel_case_to_sentence(exception.__class__.__name__)


def _title_case_to_lower_case(identifier: str) -> str:
    return identifier.lower() if identifier.istitle() else identifier


def _camel_case_to_sentence(identifier: str) -> str:
    matches = finditer(
        ".+?(?:(?<=[a-z])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])|$)", identifier
    )
    capital_words = list(map(lambda match: match.group(0), matches))
    first_word = capital_words[:1]
    other_words = capital_words[1:]
    sentence = chain(first_word, map(_title_case_to_lower_case, other_words))
    return " ".join(sentence)
