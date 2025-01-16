import click
from rich_click import RichGroup
from tabulate import tabulate

from ..tpch.databases import meta
from . import CONTEXT_SETTINGS


@click.group(
    name="power",
    cls=RichGroup,
    invoke_without_command=False,
    context_settings=CONTEXT_SETTINGS,
)
@click.pass_context
def cli(ctx: click.Context):
    """Manage Powertest results."""
    _engine = meta.setup_database()
    ctx.obj["rm"] = meta.TestResultManager(_engine)


@cli.command("list")
@click.option(
    "-t",
    "--type",
    "type_",
    type=click.Choice(["mysql", "pg"]),
    default=None,
    help="DB type",
)
@click.pass_obj
def ls(ctx, type_: str):
    """List finished tests."""
    rm: meta.TestResultManager = ctx["rm"]

    results = rm.get_powertests(db_type=type_)
    report = []
    record: meta.PowerTest
    for record in results:
        report.append(
            (
                record.id,
                record.db_type,
                record.testtime.strftime("%Y-%m-%d %H:%M:%S"),
                record.success,
                record.runtime,
                record.scale,
            )
        )

    print(
        tabulate(
            report,
            tablefmt="psql",
            headers=["ID", "DB", "Date", "Success", "Runtime (s)", "Scale"],
        )
    )


cli.add_command(ls)


@cli.command()
def delete():
    """Delete a test record."""
    pass


@cli.command()
def read():
    """Display result data of a specific test."""
    pass


@cli.command()
def show():
    """Show result data and result set of a specific test."""
    pass


if __name__ == "__main__":
    cli()
