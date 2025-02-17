import sys

import click
from rich_click import RichGroup
from tabulate import tabulate

from .. import logger, meta
from ..tpch.databases import base
from . import CONTEXT_SETTINGS
from .utils import get_db, get_db_manager


@click.group(
    name="run",
    cls=RichGroup,
    invoke_without_command=False,
    context_settings=CONTEXT_SETTINGS,
)
@click.pass_context
def cli(ctx: click.Context):
    """Manage test results."""
    _engine = meta.setup_database()
    ctx.obj["dbm"] = meta.DBManager(_engine)


@cli.command("query")
@click.option("-d", "--db", "db_id")
@click.option("-a", "--alias", "alias_", help="Database alias")
@click.option(
    "--report/--no-report",
    default=True,
    help="Save query test result (default: yes).",
)
@click.argument("query", type=int)
@click.pass_obj
def run_query(ctx, query: int, alias_: str, db_id: int, report: bool) -> None:
    """Run a TPC-H query.

    QUERY: TPC-H query number.
    """
    dbm: meta.DBManager = ctx["dbm"]
    db = get_db(dbm, id=db_id, alias_=alias_)
    db_manager: base.TPCH_Runner = get_db_manager(db)

    try:
        result, _, _ = db_manager.run_query(query_index=query, no_report=not report)
        ok, rowcount, rset, columns, result_file = result
        if ok:
            click.echo(f"Query {query} executed successfully, row count: {rowcount}.")
            if report:
                click.echo(f"Result saved to {result_file}.")
            click.echo("\n" + "-" * 55 + "\n")
            if len(rset) > 25:
                input("\nPress Enter to continue...")
            click.echo("Query Result:")
            click.echo(tabulate(rset, headers=columns, tablefmt="psql"))
        else:
            click.echo(f"Query {query} execution failed.")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


@cli.command("powertest")
@click.option("-d", "--db", "db_id")
@click.option("-a", "--alias", "alias", help="Database alias")
@click.option(
    "--report/--no-report",
    default=True,
    help="Save query test result (default: yes).",
)
@click.option("-s", "--scale", default="small", help="Data scale")
@click.pass_obj
def run_powertest(ctx, alias: str, db_id: int, report: bool, scale: str) -> None:
    """Run a TPC-H power test."""
    dbm: meta.DBManager = ctx["dbm"]
    db = get_db(dbm, id=db_id, alias_=alias)
    db_manager: base.TPCH_Runner = get_db_manager(db, scale=scale)

    try:
        db_manager.power_test(no_report=not report)  # type: ignore
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)
