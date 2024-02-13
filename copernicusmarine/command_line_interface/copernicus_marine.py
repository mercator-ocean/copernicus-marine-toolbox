import click

from copernicusmarine.command_line_interface.group_describe import (
    cli_group_describe,
)
from copernicusmarine.command_line_interface.group_get import cli_group_get
from copernicusmarine.command_line_interface.group_login import cli_group_login
from copernicusmarine.command_line_interface.group_subset import (
    cli_group_subset,
)


@click.command(
    cls=click.CommandCollection,
    sources=[
        cli_group_describe,
        cli_group_login,
        cli_group_subset,
        cli_group_get,
    ],
    context_settings=dict(help_option_names=["-h", "--help"]),
)
@click.version_option(None, "-V", "--version", package_name="copernicusmarine")
def base_command_line_interface():
    pass


def command_line_interface():
    base_command_line_interface(windows_expand_args=False)


if __name__ == "__main__":
    command_line_interface()
