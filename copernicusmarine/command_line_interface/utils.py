import pathlib

import click
from click import Context, Option, UsageError
from click.core import ParameterSource

from copernicusmarine.core_functions import documentation_utils
from copernicusmarine.core_functions.click_custom_class import (
    CustomDeprecatedClickOption,
)


class MutuallyExclusiveOption(Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop("mutually_exclusive", []))
        help = kwargs.get("help", "")
        if self.mutually_exclusive:
            ex_str = ", ".join(sorted(self.mutually_exclusive))
            kwargs["help"] = (
                help
                + """
                 NOTE: This argument is mutually exclusive with arguments: ["""
                + ex_str
                + "]."
            )

        super().__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        excluded_option_to_snake_case = {
            name.replace("-", "_") for name in self.mutually_exclusive
        }
        if (
            excluded_option_to_snake_case.intersection(opts)
            and self.name
            and self.name in opts
        ):
            raise UsageError(
                f"Illegal usage: arguments '{self.name.replace('_', '-')}' and "
                f"'{', '.join(self.mutually_exclusive)}'"
                " are mutually exclusive."
            )

        return super().handle_parse_result(ctx, opts, args)


class OtherOptionsPassedWithCreateTemplate(Exception):
    """
    Exception raised when other options are passed with create_template.

    Please note that create_template should be passed with no other option
    except log_level.
    """

    pass


def assert_cli_args_are_not_set_except_create_template(
    context: Context,
) -> None:
    for key in context.params:
        if key not in ["create_template", "log_level"]:
            parameter_source = context.get_parameter_source(key)
            if parameter_source != ParameterSource.DEFAULT:
                raise OtherOptionsPassedWithCreateTemplate(key)


tqdm_disable_option = click.option(
    "--disable-progress-bar",
    is_flag=True,
    default=False,
    help=documentation_utils.SHARED["DISABLE_PROGRESS_BAR_HELP"],
)

force_dataset_version_option = click.option(
    "--dataset-version",
    type=str,
    default=None,
    help=documentation_utils.SHARED["DATASET_VERSION_HELP"],
)

force_dataset_part_option = click.option(
    "--dataset-part",
    type=str,
    default=None,
    help=documentation_utils.SHARED["DATASET_PART_HELP"],
)

credentials_file_option = click.option(
    "--credentials-file",
    type=click.Path(path_type=pathlib.Path),
    help=documentation_utils.SHARED["CREDENTIALS_FILE_HELP"],
)

force_download_option = click.option(
    "--force-download",
    is_flag=True,
    default=False,
    hidden=True,
    cls=CustomDeprecatedClickOption,
    custom_deprecated=["--force-download"],
)
