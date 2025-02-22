import logging
import sys

import click
from rich_click import RichGroup

from ..tpch import all_tables
from ..tpch.injection import data_gen_batch
from . import CONTEXT_SETTINGS
from .db_commands import cli as dbcli
from .power_commands import cli as powercli
from .result_commands import cli as resultcli
from .run_commands import cli as runcli

logger = logging.getLogger(__name__)


@click.group(cls=RichGroup, context_settings=CONTEXT_SETTINGS)
@click.option("-v", "verbose", is_flag=True, help="Set for verbose output")
@click.pass_context
def cli(ctx: click.Context, verbose: bool):
    """TPC-H Benchmark Runner CLI tool"""
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


@cli.command("generate")
@click.option(
    "-s",
    "--scale",
    default="1",
    help="Scale factor",
)
@click.option(
    "-t",
    "--table",
    default="all",
    required=False,
    help="Table to generate",
)
def generate(scale, table) -> None:
    """Generate TPC-H test data set."""
    if table != "all" and table not in all_tables:
        logger.error(f"Invalid table {table}.")
        logger.error(f"Supported tables: {', '.join(all_tables)}.")
        sys.exit(1)

    try:
        ok, result = data_gen_batch(table, sf=int(scale))
        if not ok:
            logger.error("Data generation failed.\n")
            click.echo(f"dbgen fails, error: {result.splitlines() if result else 'n/a'}.")
            sys.exit(1)
        else:
            click.echo(
                f"succeeds, output: {result.splitlines() if result else 'done'}", err=True
            )
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


cli.add_command(dbcli)
cli.add_command(resultcli)
cli.add_command(powercli)
cli.add_command(runcli)


def main():
    cli()


if __name__ == "main":
    main()
