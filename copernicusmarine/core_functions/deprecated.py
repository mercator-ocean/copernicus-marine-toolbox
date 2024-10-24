import functools
import inspect
import logging
from typing import Any, Callable, Dict

import click

from copernicusmarine.core_functions.deprecated_options import (
    DEPRECATED_OPTIONS,
    DeprecatedOptionMapping,
)

logger = logging.getLogger("copernicus_marine_root_logger")


def get_deprecated_message(
    old_value,
    preferred_value,
    deleted_for_v2: bool = False,
    deprecated_for_v2: bool = False,
    only_for_v2: bool = False,
):
    message = ""
    if only_for_v2:
        message = f"Deprecation warning for option '{old_value}'. "
    else:
        message = f"'{old_value}' has been deprecated. "
    if old_value != preferred_value and not only_for_v2:
        message += f"Use '{preferred_value}' instead. "
    if deleted_for_v2:
        message += (
            "This option will no longer be "
            + "available in copernicusmarine>=2.0.0. "
            + "Please refer to the documentation when the new major "
            + "version is released for more information."
        )
    if deprecated_for_v2:
        message += (
            "This option will be deprecated in copernicusmarine>=2.0.0 i.e. "
            + "it will not break but it might have an unexpected effect."
        )
    return message


def log_deprecated_message(
    old_value,
    preferred_value,
    deleted_for_v2: bool,
    deprecated_for_v2: bool,
    only_for_v2: bool,
):
    logger.warning(
        get_deprecated_message(
            old_value,
            preferred_value,
            deleted_for_v2=deleted_for_v2,
            deprecated_for_v2=deprecated_for_v2,
            only_for_v2=only_for_v2,
        )
    )


def raise_both_old_and_new_value_error(old_value, new_value):
    raise TypeError(
        f"Received both {old_value} and {new_value} as arguments! "
        f"{get_deprecated_message(old_value, new_value)}"
    )


class DeprecatedClickOption(click.Option):
    def __init__(self, *args, **kwargs):
        self.deprecated = kwargs.pop("deprecated", ())
        self.preferred = kwargs.pop("preferred", args[0][-1])
        super().__init__(*args, **kwargs)


class DeprecatedClickOptionsCommand(click.Command):
    def make_parser(self, ctx):
        parser = super().make_parser(ctx)

        # get the parser options
        options = set(parser._short_opt.values())
        options |= set(parser._long_opt.values())

        # get name of the command
        command_name = ctx.command.name

        for option in options:

            def make_process(an_option):
                orig_process = an_option.process
                deprecated = getattr(an_option.obj, "deprecated", [])
                preferred = getattr(an_option.obj, "preferred", [])

                def process(value, state):
                    frame = inspect.currentframe()
                    try:
                        opt = frame.f_back.f_locals.get("opt")
                    finally:
                        del frame
                    old_alias = opt.replace("--", "").replace("-", "_")  # type: ignore
                    if (
                        opt in deprecated
                        or old_alias
                        in DEPRECATED_OPTIONS.deprecated_options_by_old_names
                    ):
                        alias_info = (
                            DEPRECATED_OPTIONS.deprecated_options_by_old_names[
                                old_alias
                            ]
                        )
                        if command_name in alias_info.targeted_functions:
                            log_deprecated_message(
                                opt,
                                preferred,
                                alias_info.deleted_for_v2,
                                alias_info.deprecated_for_v2,
                                alias_info.only_for_v2,
                            )
                    return orig_process(value, state)

                return process

            option.process = make_process(option)

        return parser


def deprecated_python_option(
    deprecated_option: DeprecatedOptionMapping,
) -> Callable:
    def deco(f: Callable):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            rename_kwargs(f.__name__, kwargs, deprecated_option)
            return f(*args, **kwargs)

        return wrapper

    return deco


def rename_kwargs(
    func_name: str, kwargs: Dict[str, Any], aliases: DeprecatedOptionMapping
):
    for old, alias_info in aliases.deprecated_options_by_old_names.items():
        if func_name not in alias_info.targeted_functions:
            continue
        new = alias_info.new_name
        if old in kwargs:
            if new in kwargs and old != new:
                raise_both_old_and_new_value_error(old, new)
            log_deprecated_message(
                old,
                new,
                alias_info.deleted_for_v2,
                alias_info.deprecated_for_v2,
                alias_info.only_for_v2,
            )
            if alias_info.replace:
                kwargs[new] = kwargs.pop(old)
