import logging

import click
from rich_click import RichGroup

from . import CONTEXT_SETTINGS
from .db_commands import cli as dbcli
from .power_commands import cli as powercli
from .result_commands import cli as resultcli
from .run_commands import cli as runcli
from .server_commands import cli as servercli

logger = logging.getLogger(__name__)


@click.group(cls=RichGroup, context_settings=CONTEXT_SETTINGS)
@click.option("-v", "verbose", is_flag=True, help="Set for verbose output")
@click.pass_context
def cli(ctx: click.Context, verbose: bool):
    """Rapids Installer CLI tool"""
    ctx.ensure_object(dict)
    ctx.obj = {"verbose": verbose}
    if verbose:
        root_logger = logging.getLogger()
        for handler in root_logger.handlers:
            handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
        logger.debug(f"Verbose mode is on for {logger.name}.")


@cli.command("version")
def version() -> None:
    """Show Transformer version information."""
    click.echo("TPC-H Runner v1.0")


def main():
    cli.add_command(dbcli)
    cli.add_command(servercli)
    cli.add_command(resultcli)
    cli.add_command(powercli)
    cli.add_command(runcli)
    cli()


if __name__ == "main":
    main()
