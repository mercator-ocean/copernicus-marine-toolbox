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


class CustomClickOptionsContext(click.Context):
    @property
    def command_path(self):
        if (
            self.info_name == "split-on"
            and self.parent is not None
            and self.parent.command.name == "subset"
        ):
            return self.parent.command_path
        return super().command_path


_original_context_command_path = click.Context.command_path


def _patched_command_path(self):
    if (
        self.info_name == "split-on"
        and self.parent is not None
        and getattr(self.parent.command, "name", None) == "subset"
    ):
        return self.parent.command_path
    return _original_context_command_path.__get__(self, type(self))


if not getattr(click.Context, "_copernicus_custom_command_path", False):
    click.Context.command_path = property(_patched_command_path)
    click.Context._copernicus_custom_command_path = True


class CustomClickOptionsCommand(click.Command):
    context_class = CustomClickOptionsContext

    def make_parser(self, ctx: Context):
        parser = super().make_parser(ctx)

        # get the parser options
        options = set(parser._short_opt.values())
        options |= set(parser._long_opt.values())

        for option in options:
            option.process = _wrap_option_process(option)

        return parser

    def _split_on_usage(self, ctx: Context) -> str | None:
        if self.name != "split-on":
            return None

        root = ctx.find_root()
        root_name = getattr(root, "info_name", None) or "subset"
        parent = getattr(ctx, "parent", None)
        parent_name = getattr(getattr(parent, "command", None), "name", None)

        if parent_name == "subset":
            return (
                f"Usage: {root_name} {parent_name} [SUBSET OPTIONS] "
                f"{self.name} [SPLIT-ON OPTIONS]\n"
            )

        if parent_name is None:
            return (
                f"Usage: {root_name or 'subset'} [SUBSET OPTIONS] "
                f"{self.name} [SPLIT-ON OPTIONS]\n"
            )

        return None

    def collect_usage_pieces(self, ctx: Context):
        if self.name == "split-on":
            parent = getattr(ctx, "parent", None)
            parent_name = getattr(
                getattr(parent, "command", None), "name", None
            )
            if parent_name == "subset" or parent_name is None:
                return ["[SUBSET OPTIONS]", "split-on", "[SPLIT-ON OPTIONS]"]
        return super().collect_usage_pieces(ctx)

    def format_usage(self, ctx: Context, formatter):
        usage = self._split_on_usage(ctx)
        if usage is not None:
            formatter.write(usage)
            return
        return super().format_usage(ctx, formatter)

    def get_usage(self, ctx: Context):
        usage = self._split_on_usage(ctx)
        if usage is not None:
            return usage
        return super().get_usage(ctx)

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
