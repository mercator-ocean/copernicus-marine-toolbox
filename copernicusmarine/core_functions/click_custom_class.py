import inspect
import logging
import sys
from typing import Callable

import click
from click import Context

from copernicusmarine.core_functions.deprecated_options import (
    log_deprecated_message,
)

logger = logging.getLogger("copernicusmarine")


def _wrap_option_process(option) -> Callable:
    orig_process = option.process
    is_deprecated = isinstance(option.obj, CustomDeprecatedClickOption)
    custom_deprecated = getattr(option.obj, "custom_deprecated", ())
    preferred = getattr(option.obj, "preferred", None)
    allow_multiple = bool(
        getattr(option.obj, "multiple", False)
        or getattr(option.obj, "count", None)
    )

    if is_deprecated:
        msg = "Expected `deprecated` value for `{}`"
        assert custom_deprecated is not None, msg.format(option.obj.name)

    def process(value, state):
        frame = inspect.currentframe()
        try:
            if frame and frame.f_back:
                opt = frame.f_back.f_locals.get("opt")
        finally:
            del frame
        if not allow_multiple and option.dest in state.opts:
            option_repr = ", ".join(option.obj.opts)
            raise click.UsageError(
                f"Option '{option_repr}' was provided multiple times."
            )
        if is_deprecated and opt in custom_deprecated:
            log_deprecated_message(opt, preferred)
        return orig_process(value, state)

    return process


class CustomDeprecatedClickOption(click.Option):
    def __init__(self, *args, **kwargs):
        self.custom_deprecated = kwargs.pop("custom_deprecated", ())
        self.preferred = kwargs.pop("preferred", None)
        super().__init__(*args, **kwargs)


class CustomClickOptionsCommand(click.Command):
    def make_parser(self, ctx: Context):
        parser = super().make_parser(ctx)

        # get the parser options
        options = set(parser._short_opt.values())
        options |= set(parser._long_opt.values())

        for option in options:
            option.process = _wrap_option_process(option)

        return parser

    def format_epilog(self, ctx, formatter):
        if self.epilog:
            formatter.write_paragraph()
            for line in self.epilog.split("\n"):
                if ".. code-block::" in line:
                    continue
                formatter.write(line + "\n")


class CustomClickOptionsGroup(click.Group):
    def make_parser(self, ctx):
        parser = super().make_parser(ctx)

        options = set(parser._short_opt.values())
        options |= set(parser._long_opt.values())

        # so that we can skip validation of the subset options
        # in case help is requested for split-on
        target = "split-on"
        argv = sys.argv[1:]
        ctx.meta["help_for"] = None
        if target in argv:
            ctx.meta["command"] = target
            idx = argv.index(target)
            after = argv[idx + 1 :]
            help_flags = {"-h", "--help"}
            if any(token in help_flags for token in after):
                ctx.meta["help_for"] = target

        for option in options:
            option.process = _wrap_option_process(option)

        return parser

    def format_epilog(self, ctx, formatter):
        if self.epilog:
            formatter.write_paragraph()
            for line in self.epilog.split("\n"):
                if ".. code-block::" in line:
                    continue
                formatter.write(line + "\n")
