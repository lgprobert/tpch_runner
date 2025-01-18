from typing import TYPE_CHECKING, Union

import click
from rich_click import RichGroup
from tabulate import tabulate

from .. import meta
from . import CONTEXT_SETTINGS

if TYPE_CHECKING:
    from ..tpch.databases.mysqldb import MySQL_TPCH, MySQLDB
    from ..tpch.databases.pgdb import PG_TPCH, PGDB


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
    ctx.obj["rm"] = meta.TestResultManager(_engine)


@cli.command("query")
@click.option(
    "-t",
    "--type",
    "type_",
    type=click.Choice(["mysql", "pg"]),
    default="mysql",
    help="DB type",
)
@click.option(
    "--report/--no-report",
    default=True,
    help="Save query test result (default: yes).",
)
@click.argument("query", type=int)
def run_query(query: int, type_: str, report: bool) -> None:
    runner: Union[MySQL_TPCH, PG_TPCH]
    conn: Union[MySQLDB, PGDB]

    if type_ == "mysql":
        from ..tpch.databases.mysqldb import MySQL_TPCH, MySQLDB

        conn = MySQLDB(
            host="localhost", port=3306, db_name="tpch", user="root", password="admintest"
        )
        runner = MySQL_TPCH(conn)
    elif type_ == "pg":
        from ..tpch.databases.pgdb import PG_TPCH, PGDB

        conn = PGDB(
            host="localhost",
            port=5432,
            db_name="tpch",
            user="rapids",
            password="rdpadmin",
        )
        runner = PG_TPCH(conn)
    else:
        raise ValueError("Unsupported DB type")

    result, _, _ = runner.run_query(query_index=query, no_report=not report)
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


@cli.command("powertest")
@click.option(
    "-t",
    "--type",
    "type_",
    type=click.Choice(["mysql", "pg"]),
    default="mysql",
    help="DB type",
)
@click.option(
    "--report/--no-report",
    default=True,
    help="Save query test result (default: yes).",
)
@click.option("-s", "--scale", default="small", help="Data scale")
def run_powertest(type_: str, report: bool, scale: str) -> None:
    runner: Union[MySQL_TPCH, PG_TPCH]
    conn: Union[MySQLDB, PGDB]

    if type_ == "mysql":
        from ..tpch.databases.mysqldb import MySQL_TPCH, MySQLDB

        conn = MySQLDB(
            host="localhost", port=3306, db_name="tpch", user="root", password="admintest"
        )
        runner = MySQL_TPCH(conn, scale)
    elif type_ == "pg":
        from ..tpch.databases.pgdb import PG_TPCH, PGDB

        conn = PGDB(
            host="localhost",
            port=5432,
            db_name="tpch",
            user="rapids",
            password="rdpadmin",
        )
        runner = PG_TPCH(conn, scale)
    else:
        raise ValueError("Unsupported DB type")

    runner.power_test(no_report=not report)
