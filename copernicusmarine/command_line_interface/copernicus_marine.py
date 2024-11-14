import multiprocessing

multiprocessing.freeze_support()

import click

from copernicusmarine.command_line_interface.group_describe import cli_describe
from copernicusmarine.command_line_interface.group_get import cli_get
from copernicusmarine.command_line_interface.group_login import cli_login
from copernicusmarine.command_line_interface.group_subset import cli_subset

multiprocessing.freeze_support()  # TODO: check if we can remove some of the calls


@click.command(
    cls=click.CommandCollection,
    sources=[
        cli_describe,
        cli_login,
        cli_subset,
        cli_get,
    ],
    context_settings=dict(help_option_names=["-h", "--help"]),
)
@click.version_option(None, "-V", "--version", package_name="copernicusmarine")
def base_command_line_interface():
    pass


def command_line_interface():
    base_command_line_interface(windows_expand_args=False)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    command_line_interface()
