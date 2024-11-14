import inspect
import logging

import click

from copernicusmarine.core_functions.deprecated_options import (
    log_deprecated_message,
)

logger = logging.getLogger("copernicusmarine")


class DeprecatedClickOption(click.Option):
    def __init__(self, *args, **kwargs):
        self.deprecated = kwargs.pop("deprecated", ())
        self.preferred = kwargs.pop("preferred", None)
        super().__init__(*args, **kwargs)


class CustomClickOptionsCommand(click.Command):
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
                        if frame and frame.f_back:
                            opt = frame.f_back.f_locals.get("opt")
                    finally:
                        del frame

                    if opt in deprecated:
                        log_deprecated_message(opt, preferred)
                    return orig_process(value, state)

                return process

            option.process = make_process(option)

        return parser

    def format_epilog(self, ctx, formatter):
        if self.epilog:
            formatter.write_paragraph()
            for line in self.epilog.split("\n"):
                if ".. code-block::" in line:
                    continue
                formatter.write(line + "\n")
