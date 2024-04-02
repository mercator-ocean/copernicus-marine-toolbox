import functools
import inspect
import logging
from typing import Any, Callable, Dict

import click

logger = logging.getLogger("copernicus_marine_root_logger")


def get_deprecated_message(old_value, preferred_value):
    return (
        f"'{old_value}' has been deprecated, use '{preferred_value}' instead"
    )


def log_deprecated_message(old_value, preferred_value):
    logger.warning(get_deprecated_message(old_value, preferred_value))


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

        for option in options:
            if not isinstance(option.obj, DeprecatedClickOption):
                continue

            def make_process(an_option):
                orig_process = an_option.process
                deprecated = getattr(an_option.obj, "deprecated", None)
                preferred = getattr(an_option.obj, "preferred", None)
                msg = "Expected `deprecated` value for `{}`"
                assert deprecated is not None, msg.format(an_option.obj.name)

                def process(value, state):
                    frame = inspect.currentframe()
                    try:
                        opt = frame.f_back.f_locals.get("opt")
                    finally:
                        del frame

                    if opt in deprecated:
                        log_deprecated_message(opt, preferred)
                    return orig_process(value, state)

                return process

            option.process = make_process(option)

        return parser


def deprecated_python_option(**aliases: str) -> Callable:
    def deco(f: Callable):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            rename_kwargs(f.__name__, kwargs, aliases)
            return f(*args, **kwargs)

        return wrapper

    return deco


def rename_kwargs(
    func_name: str, kwargs: Dict[str, Any], aliases: Dict[str, str]
):
    for alias, new in aliases.items():
        if alias in kwargs:
            if new in kwargs:
                raise_both_old_and_new_value_error(alias, new)
            log_deprecated_message(alias, new)
            kwargs[new] = kwargs.pop(alias)
