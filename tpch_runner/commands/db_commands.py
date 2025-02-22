import sys
from pathlib import Path

import click
from rich_click import RichGroup
from tabulate import tabulate

from .. import logger, meta
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
    if ctx.obj is None:
        ctx.obj = {}
    if "rm" not in ctx.obj:
        _engine = meta.setup_database()
        ctx.obj["rm"] = meta.DBManager(_engine)


@cli.command("list")
@click.pass_obj
def ls(ctx) -> None:
    """List configured databases connections."""
    try:
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
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


@cli.command("add")
@click.option(
    "-t",
    "--type",
    "db_type",
    type=click.Choice(supported_databases),
    default="mysql",
    help="DB type",
)
@click.option("-H", "--host", default="localhost", help="Database host")
@click.option("-p", "--port", default="", help="Database port")
@click.option("-u", "--user", default="", help="Database user")
@click.option("-w", "--password", default="", help="Database password")
@click.option("-d", "--db", "dbname", default="", help="Database name")
@click.option("-a", "--alias", help="Database alias")
@click.option("-W", "cli_password", is_flag=True, help="Enter password on command line.")
@click.pass_obj
def add(
    ctx, db_type, host, port: str, user, password, dbname, alias, cli_password
) -> None:
    """Add a new database connection."""
    try:
        rm: meta.DBManager = ctx["rm"]
        if not port.isdigit():  # it is a local file db like duckdb
            port = str(Path(port).expanduser())
        if cli_password:
            password = click.prompt("Enter database password", hide_input=True)
        rm.add_database(db_type, host, port, user, password, dbname, alias)
        click.echo("Database added successfully.")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


@cli.command("delete")
@click.argument("db_id", required=False, type=int)
@click.option("-a", "--alias", "_alias", help="Database alias")
@click.pass_obj
def delete(ctx, db_id, _alias) -> None:
    """Delete a database connection.

    DB_ID: database ID
    """
    try:
        rm: meta.DBManager = ctx["rm"]
        if not db_id and not _alias:
            click.echo("Either database ID or alias is required.")
            sys.exit(1)
        rm.delete_database(db_id=db_id, alias=_alias)
        click.echo("Database deleted successfully.")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


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
@click.option("-H", "--host", help="Database host")
@click.option("-p", "--port", help="Database port")
@click.option("-u", "--user", help="Database user")
@click.option("-w", "--password", help="Database password")
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
    try:
        rm: meta.DBManager = ctx["rm"]
        if not db_id and not alias:
            click.echo("Either database ID or alias is required.")
            sys.exit(1)
        if cli_password:
            password = click.prompt("Enter database password", hide_input=True)
        rm.update_database(db_id, db_type, host, port, user, password, dbname, alias)
        click.echo("Database updated successfully.")
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


@cli.command("tables")
@click.argument("db_id", required=False, type=int)
@click.option("-a", "--alias", "alias", help="Database alias")
@click.pass_obj
def tables(ctx, db_id, alias) -> None:
    """List tables in a database.

    DB_ID: database ID
    """
    try:
        rm: meta.DBManager = ctx["rm"]
        if not db_id and not alias:
            click.echo("Either database ID or alias is required.")
            sys.exit(1)
        tables = rm.list_tables(db_id=db_id, alias=alias)
        print(tabulate(tables, tablefmt="psql", headers=["Table"]))
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


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
    try:
        db_manager: base.TPCH_Runner = get_db_manager(db)
        db_manager.create_tables()
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


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
@click.option("-d", "--delimiter", default=",", help="Column delimiter")
@click.option(
    "--optimize/--no-optimize",
    default=True,
    help="Optimize MySQL for batch data loading.",
)
@click.option("-r", "--reindex", is_flag=True, default=False, help="Recreate index")
@click.pass_obj
def load(
    ctx, db_id, alias, table, data_folder, delimiter, optimize: bool, reindex: bool
) -> None:
    """Load specified table or all tables.

    DB_ID: database ID
    """
    rm: meta.DBManager = ctx["rm"]
    db = get_db(rm, id=db_id, alias_=alias)
    db_manager: base.TPCH_Runner = get_db_manager(db)

    try:
        if optimize:
            logger.info("Running before load optimization.")
            db_manager.before_load(reindex=reindex)
        if table:
            db_manager.load_single_table(
                table, data_folder=data_folder, delimiter=delimiter
            )
        else:
            db_manager.load_data(data_folder=data_folder, delimiter=delimiter)

        if optimize:
            logger.info("Running after load optimization.")
            db_manager.after_load(reindex=reindex)
    except Exception as e:
        logger.error(f"Error during load: {str(e)}")
        sys.exit(1)


@cli.command("reload")
@click.argument("db_id", required=False, type=int)
@click.option("-a", "--alias", "alias", help="Database alias")
@click.option(
    "--optimize/--no-optimize",
    default=True,
    help="Optimize MySQL for batch data loading.",
)
@click.pass_obj
def reload(ctx, db_id, alias, optimize: bool) -> None:
    """Reload all tables, truncate before reload.

    DB_ID: database ID
    """
    rm: meta.DBManager = ctx["rm"]
    db = get_db(rm, id=db_id, alias_=alias)
    try:
        db_manager: base.TPCH_Runner = get_db_manager(db)
        db_manager.truncate_table()
        db_manager.load_data(optimize=optimize)
        db_manager.after_load()
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


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
    try:
        db_manager: base.TPCH_Runner = get_db_manager(db)
        if not table:
            table = "all"
        db_manager.truncate_table(table)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


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
    try:
        db_manager: base.TPCH_Runner = get_db_manager(db)

        if not table:
            db_manager.drop_table()
        else:
            db_manager.drop_table(table)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


@cli.command("count")
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
def count_rows(ctx, alias, table) -> None:
    """Count and show number of rows in specified table or all tables."""
    if table not in all_tables:
        table = "all"
    rm: meta.DBManager = ctx["rm"]
    db = get_db(rm, alias_=alias)
    try:
        db_manager: base.TPCH_Runner = get_db_manager(db)

        table_data = db_manager.count_rows(table)
        table_rowcounts = []
        for tbl, rowcount in zip(table_data.keys(), table_data.values()):
            table_rowcounts.append((tbl, rowcount))

        print(tabulate(table_rowcounts, tablefmt="psql", headers=["Table", "RowCount"]))
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)
