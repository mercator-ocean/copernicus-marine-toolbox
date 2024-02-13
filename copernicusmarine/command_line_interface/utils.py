import click
from click import Context, Option, UsageError
from click.core import ParameterSource

from copernicusmarine.core_functions.deprecated import DeprecatedClickOption


class MutuallyExclusiveOption(Option):
    def __init__(self, *args, **kwargs):
        self.mutually_exclusive = set(kwargs.pop("mutually_exclusive", []))
        help = kwargs.get("help", "")
        if self.mutually_exclusive:
            ex_str = ", ".join(self.mutually_exclusive)
            kwargs["help"] = (
                help
                + """
                \b\nNOTE: This argument is mutually exclusive with arguments: ["""
                + ex_str
                + "]."
            )

        super().__init__(*args, **kwargs)

    def handle_parse_result(self, ctx, opts, args):
        if self.mutually_exclusive.intersection(opts) and self.name in opts:
            raise UsageError(
                f"Illegal usage: `{self.name.replace('_', '-')}` "
                "is mutually exclusive with "
                f"arguments `{', '.join(self.mutually_exclusive).replace('_', '-')}`."
            )

        return super().handle_parse_result(ctx, opts, args)


class OtherOptionsPassedWithCreateTemplate(Exception):
    ...


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
    help="Flag to hide progress bar.",
)

force_dataset_version_option = click.option(
    "--dataset-version",
    "--force-dataset-version",
    cls=DeprecatedClickOption,
    deprecated=["--force-dataset-version"],
    preferred="--dataset-version",
    type=str,
    default=None,
    help="Force the selection of a specific dataset version.",
)

force_dataset_part_option = click.option(
    "--dataset-part",
    "--force-dataset-part",
    cls=DeprecatedClickOption,
    deprecated=["--force-dataset-part"],
    preferred="--dataset-part",
    type=str,
    default=None,
    help="Force the selection of a specific dataset part.",
)
