import logging

import click
from rich_click import RichGroup

from .. import logger
from . import CONTEXT_SETTINGS
from .power_commands import cli as powercli
from .result_commands import cli as resultcli
from .server_commands import cli as servercli


@click.group(cls=RichGroup, context_settings=CONTEXT_SETTINGS)
@click.option("-v", "verbose", is_flag=True, help="Set for verbose output")
@click.pass_context
def cli(ctx: click.Context, verbose: bool):
    """Rapids Installer CLI tool"""
    ctx.obj = {"verbose": verbose}
    # ctx.obj["app"] = app
    if verbose:
        for handler in logger.handlers:
            if isinstance(handler, logging.StreamHandler):
                handler.setLevel(logging.DEBUG)
            elif isinstance(handler, logging.FileHandler):
                handler.setLevel(logging.DEBUG)
        logger.setLevel(logging.DEBUG)
        logger.debug(f"Verbose mode is on for {logger.name}.")


@cli.command("log")
@click.pass_obj
def log(ctx: dict):
    """Test logger capabilities"""

    click.echo(f"verbose level: {ctx['verbose']}")
    click.echo(f"current logger level: {logger.level}")
    logger.info("info log")
    logger.warning("warning log")
    logger.error("error log")
    logger.debug("debug log")


@cli.command("version")
def version() -> None:
    """Show Transformer version information."""
    click.echo("TPC-H Runner v1.0")


def main():
    cli.add_command(servercli)
    cli.add_command(resultcli)
    cli.add_command(powercli)
    cli()


if __name__ == "main":
    main()
