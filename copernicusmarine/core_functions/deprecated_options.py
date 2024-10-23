import functools
from collections.abc import Iterator, Mapping
from typing import Any, Callable, Optional

from copernicusmarine.core_functions.utils import logger


class DeprecatedOption:
    def __init__(self, old_name, new_name, replace=True) -> None:
        self.old_name = old_name
        self.new_name = new_name
        self.replace = replace


class DeprecatedOptionMapping(Mapping):
    def __init__(self, deprecated_options: list[DeprecatedOption]) -> None:
        self.deprecated_options_by_old_names: dict = {}
        for value in deprecated_options:
            if value not in self.deprecated_options_by_old_names:
                self.deprecated_options_by_old_names[value.old_name] = value

    def __getitem__(self, __key: str) -> DeprecatedOption:
        return self.deprecated_options_by_old_names[__key]

    def __iter__(self) -> Iterator:
        return self.deprecated_options_by_old_names.__iter__()

    def __len__(self) -> int:
        return self.deprecated_options_by_old_names.__len__()

    @property
    def dict_old_names_to_new_names(self):
        result_dict = {}
        for (
            old_name,
            deprecated_option,
        ) in self.deprecated_options_by_old_names.items():
            if deprecated_option.replace:
                result_dict[old_name] = deprecated_option.new_name
        return result_dict


DEPRECATED_OPTIONS: DeprecatedOptionMapping = DeprecatedOptionMapping(
    [
        DeprecatedOption(
            old_name="include_datasets",
            new_name="include_datasets",
            replace=True,
        ),
        DeprecatedOption(
            old_name="include_keywords",
            new_name="include_keywords",
            replace=True,
        ),
        DeprecatedOption(
            old_name="include_all",
            new_name="include_all",
            replace=True,
        ),
        DeprecatedOption(
            old_name="include_description",
            new_name="include_description",
            replace=True,
        ),
    ]
)


def get_deprecated_message(old_value, preferred_value):
    return (
        f"'{old_value}' has been deprecated, use '{preferred_value}' instead"
    )


def log_deprecated_message(old_value: str, preferred_value: Optional[str]):
    if preferred_value is None:
        logger.warning(f"'{old_value}' has been deprecated")
    else:
        logger.warning(get_deprecated_message(old_value, preferred_value))


def raise_both_old_and_new_value_error(old_value, new_value):
    raise TypeError(
        f"Received both {old_value} and {new_value} as arguments! "
        f"{get_deprecated_message(old_value, new_value)}"
    )


def deprecated_python_option(**aliases: str) -> Callable:
    def deco(f: Callable):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            rename_kwargs(f.__name__, kwargs, aliases)
            return f(*args, **kwargs)

        return wrapper

    return deco


def rename_kwargs(
    func_name: str, kwargs: dict[str, Any], aliases: dict[str, str]
):
    for alias, new in aliases.items():
        if alias in kwargs:
            if new in kwargs and alias != new:
                raise_both_old_and_new_value_error(alias, new)
            if alias == new:
                log_deprecated_message(alias, None)
            else:
                log_deprecated_message(alias, new)
                kwargs[new] = kwargs.pop(alias)
