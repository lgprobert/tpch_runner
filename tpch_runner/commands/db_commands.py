import sys

import click
from rich_click import RichGroup
from tabulate import tabulate

from .. import meta
from ..tpch import DATA_DIR, all_tables, supported_databases
from ..tpch.databases import base
from . import CONTEXT_SETTINGS
from .utils import get_db, get_db_manager


@click.group(
    name="db",
    cls=RichGroup,
    invoke_without_command=False,
    context_settings=CONTEXT_SETTINGS,
)
@click.pass_context
def cli(ctx: click.Context):
    """Manage database server connections."""
    _engine = meta.setup_database()
    ctx.obj["rm"] = meta.DBManager(_engine)


@cli.command("list")
@click.pass_obj
def ls(ctx) -> None:
    """List configured databases connections."""
    rm: meta.DBManager = ctx["rm"]
    headers: list[str] = ["Type", "Host", "Port", "User", "DBName", "Alias"]

    results = rm.get_databases()
    record: meta.Database
    report = []
    for record in results:
        report.append(
            (
                record.id,
                record.db_type,
                record.host,
                record.port,
                record.user,
                record.dbname,
                record.alias,
            )
        )

    print(tabulate(report, tablefmt="psql", headers=headers))


@cli.command("add")
@click.option(
    "-t",
    "--type",
    "db_type",
    type=click.Choice(supported_databases),
    default="mysql",
    help="DB type",
)
@click.option("-h", "--host", help="Database host")
@click.option("-p", "--port", help="Database port")
@click.option("-u", "--user", help="Database user")
@click.option("-W", "--password", help="Database password")
@click.option("-d", "--db", "dbname", help="Database name")
@click.option("-a", "--alias", help="Database alias")
@click.option("-W", "cli_password", is_flag=True, help="Enter password on command line.")
@click.pass_obj
def add(ctx, db_type, host, port, user, password, dbname, alias, cli_password) -> None:
    """Add a new database connection."""
    rm: meta.DBManager = ctx["rm"]
    if cli_password:
        password = click.prompt("Enter database password", hide_input=True)
    rm.add_database(db_type, host, port, user, password, dbname, alias)
    click.echo("Database added successfully.")


@cli.command("delete")
@click.argument("db_id", required=False, type=int)
@click.option("-a", "--alias", "_alias", help="Database alias")
@click.pass_obj
def delete(ctx, db_id, _alias) -> None:
    """Delete a database connection.

    DB_ID: database ID
    """
    rm: meta.DBManager = ctx["rm"]
    if not db_id and not _alias:
        click.echo("Either database ID or alias is required.")
        sys.exit(1)
    rm.delete_database(db_id=db_id, alias=_alias)
    click.echo("Database deleted successfully.")


@cli.command("update")
@click.argument("db_id", required=False, type=int, default=None)
@click.option(
    "-t",
    "--type",
    "db_type",
    type=click.Choice(supported_databases),
    default="mysql",
    help="DB type",
)
@click.option("-h", "--host", help="Database host")
@click.option("-p", "--port", help="Database port")
@click.option("-u", "--user", help="Database user")
@click.option("-W", "--password", help="Database password")
@click.option("-d", "--db", "dbname", help="Database name")
@click.option("-a", "--alias", help="Database alias")
@click.option("-W", "cli_password", is_flag=True, help="Enter password on command line.")
@click.pass_obj
def update(
    ctx, db_id, db_type, host, port, user, password, dbname, alias, cli_password
) -> None:
    """Update a database connection.

    DB_ID: database ID
    """
    rm: meta.DBManager = ctx["rm"]
    if not db_id and not alias:
        click.echo("Either database ID or alias is required.")
        sys.exit(1)
    if cli_password:
        password = click.prompt("Enter database password", hide_input=True)
    rm.update_database(db_id, db_type, host, port, user, password, dbname, alias)
    click.echo("Database updated successfully.")


@cli.command("tables")
@click.argument("db_id", required=False, type=int)
@click.option("-a", "--alias", "alias", help="Database alias")
@click.pass_obj
def tables(ctx, db_id, alias) -> None:
    """List tables in a database.

    DB_ID: database ID
    """
    rm: meta.DBManager = ctx["rm"]
    if not db_id and not alias:
        click.echo("Either database ID or alias is required.")
        sys.exit(1)
    tables = rm.list_tables(db_id=db_id, alias=alias)
    print(tabulate(tables, tablefmt="psql", headers=["Table"]))


@cli.command("create")
@click.argument("db_id", required=False, type=int)
@click.option("-a", "--alias", "alias", help="Database alias")
@click.pass_obj
def create(ctx, db_id, alias) -> None:
    """Create all TPC-H tables.

    DB_ID: database ID
    """
    rm: meta.DBManager = ctx["rm"]
    db = get_db(rm, id=db_id, alias_=alias)
    db_manager: base.TPCH_Runner = get_db_manager(db)
    db_manager.create_tables()


@cli.command("load")
@click.argument("db_id", required=False, type=int)
@click.option("-a", "--alias", "alias", help="Database alias")
@click.option(
    "-t",
    "--table",
    "table",
    type=click.Choice(all_tables),
    default=None,
    help="Table name",
)
@click.option("-p", "--path", "data_folder", default=str(DATA_DIR), help="Data folder")
@click.pass_obj
def load(ctx, db_id, alias, table, data_folder) -> None:
    """Load specified table or all tables.

    DB_ID: database ID
    """
    rm: meta.DBManager = ctx["rm"]
    db = get_db(rm, id=db_id, alias_=alias)
    db_manager: base.TPCH_Runner = get_db_manager(db)

    if table:
        db_manager.load_single_table(table, data_folder=data_folder)
    else:
        db_manager.load_data(data_folder=data_folder)


@cli.command("reload")
@click.argument("db_id", required=False, type=int)
@click.option("-a", "--alias", "alias", help="Database alias")
@click.pass_obj
def reload(ctx, db_id, alias) -> None:
    """Reload all tables, truncate before reload.

    DB_ID: database ID
    """
    rm: meta.DBManager = ctx["rm"]
    db = get_db(rm, id=db_id, alias_=alias)
    db_manager: base.TPCH_Runner = get_db_manager(db)
    db_manager.truncate_table()
    db_manager.load_data()
    db_manager.after_load()


@cli.command("truncate")
@click.argument("db_id", required=False, type=int)
@click.option("-a", "--alias", "alias", help="Database alias")
@click.option(
    "-t",
    "--table",
    "table",
    type=click.Choice(all_tables),
    default=None,
    help="Table name",
)
@click.pass_obj
def truncate(ctx, db_id, alias, table) -> None:
    """Trucate specified table or all tables.

    DB_ID: database ID
    """
    rm: meta.DBManager = ctx["rm"]
    db = get_db(rm, id=db_id, alias_=alias)
    db_manager: base.TPCH_Runner = get_db_manager(db)
    if not table:
        table = "all"
    db_manager.truncate_table(table)


@cli.command("drop")
@click.argument("db_id", required=False, type=int)
@click.option("-a", "--alias", "alias", help="Database alias")
@click.option(
    "-t",
    "--table",
    "table",
    type=click.Choice(all_tables),
    default=None,
    help="Table name",
)
@click.pass_obj
def drop(ctx, db_id, alias, table) -> None:
    """Drop specified table or all tables.

    DB_ID: database ID
    """
    rm: meta.DBManager = ctx["rm"]
    if table and table not in all_tables:
        click.echo(f"Unsupported table: {table}")
        sys.exit(1)
    db = get_db(rm, id=db_id, alias_=alias)
    db_manager: base.TPCH_Runner = get_db_manager(db)

    if not table:
        db_manager.drop_table()
    else:
        db_manager.drop_table(table)
