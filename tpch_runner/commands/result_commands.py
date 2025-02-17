import sys
from typing import Any

import click
from rich_click import RichGroup
from tabulate import tabulate

from .. import logger, meta
from ..tpch import supported_databases
from . import CONTEXT_SETTINGS
from .utils import format_datetime


@click.group(
    name="result",
    cls=RichGroup,
    invoke_without_command=False,
    context_settings=CONTEXT_SETTINGS,
)
@click.pass_context
def cli(ctx: click.Context):
    """Manage test results."""
    _engine = meta.setup_database()
    ctx.obj["rm"] = meta.TestResultManager(_engine)


@cli.command("list")
@click.option(
    "-t",
    "--type",
    "type_",
    type=click.Choice(supported_databases),
    default=None,
    help="DB type",
)
@click.option("--single", is_flag=True, help="List single run query results.")
@click.pass_obj
def ls(ctx, type_: str, single: bool):
    """List finished tests."""
    try:
        rm: meta.TestResultManager = ctx["rm"]
        headers: list[str] = [
            "Test ID",
            "DB",
            "Query",
            "Test Time",
            "Success",
            "Rowcount",
            "Runtime (s)",
        ]

        if single:
            results = rm.get_test_results(db_type=type_)
        else:
            results = rm.get_test_results_from_powertest(db_type=type_)
            headers = ["Power ID"] + headers
        record: meta.TestResult
        report = []
        for record in results:
            arow = (
                record.id,
                record.db_type,
                record.query_name,
                format_datetime(record.testtime),  # type: ignore
                record.success,
                record.rowcount,
                record.runtime,
            )
            if not single:
                arow = (record.power_test.id,) + arow  # type: ignore
            report.append(arow)

        print(tabulate(report, tablefmt="psql", headers=headers))
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


@cli.command("compare")
@click.option(
    "-s",
    "--source",
    help="Source test ID",
)
@click.option(
    "-d",
    "--dest",
    help="Destination test ID",
)
@click.pass_obj
def compare(ctx, source, dest) -> None:
    """Compare two test results."""
    try:
        rm: meta.TestResultManager = ctx["rm"]

        src_result: meta.TestResult = rm.get_test_results_from_powertest(test_id=source)[
            0
        ]
        dest_result: meta.TestResult = rm.get_test_results_from_powertest(test_id=dest)[0]

        report: list[tuple] = []
        report.append(("Database", src_result.db_type, dest_result.db_type))
        report.append(("Query", src_result.query_name, dest_result.query_name))
        report.append(("Success", src_result.success, dest_result.success))
        report.append(("Rowcount", src_result.rowcount, dest_result.rowcount))
        report.append(
            ("Runtime (s)", f"{src_result.runtime: .4f}", f"{dest_result.runtime: .4f}")
        )

        print(
            tabulate(
                report, tablefmt="psql", headers=["Attribute", "Source", "Destination"]
            )
        )
    except Exception as e:
        logger.error(f"Error: {str(e)}")
        sys.exit(1)


@cli.command("show")
@click.argument("test_id")
@click.pass_obj
def show(ctx, test_id: int):
    """Show result data and result set of a specific test.

    TEST_ID: ID of the test to show.
    """
    rm: meta.TestResultManager = ctx["rm"]

    try:
        result, query, df = rm.read_result(test_id)
        result_detail: dict[str, Any] = {}
        result_detail["ID"] = result.id
        result_detail["Database"] = result.db_type
        result_detail["Query Name"] = result.query_name
        result_detail["Test Time"] = result.testtime.strftime("%Y-%m-%d %H:%M:%S")
        result_detail["Success"] = result.success
        result_detail["Rowcount"] = result.rowcount
        result_detail["Runtime (s)"] = result.runtime
        result_detail["Result CSV"] = result.result_csv
        report = []
        for k, v in result_detail.items():
            report.append((k, v))
        print("\nTest Result Detail:")
        print(tabulate(report, tablefmt="psql", headers=["Attribute", "Value"]))
        print()

        if query.count("\n") + len(df) > 25:
            input("\nPress Enter to continue...")
        print(f"Query {result.query_name.upper()} Text:")
        print("-" * 50)
        print(query)

        if query.count("\n") > 25:
            input("\nPress Enter to view query resultset...\n")
        print("Test Result Set:")
        print("-" * 50)
        start = 0
        while start < len(df):
            end = min(start + 25, len(df))
            print(
                tabulate(
                    df.iloc[start:end],
                    headers=df.columns,
                    tablefmt="psql" + "\n" + "---" * 60,
                    showindex=False,
                )
            )
            start = end
            if start < len(df):
                input("\nPress Enter for next page...")
    except Exception as e:
        click.echo(f"Fails to show details for test ID {test_id}, exception: {e}")
        sys.exit(1)


@cli.command("delete")
@click.argument("test_id")
@click.pass_obj
def delete(ctx, test_id: int):
    """Delete a test record.

    TEST_ID: ID of the test to delete.
    """
    try:
        rm: meta.TestResultManager = ctx["rm"]
        rm.delete_test_result(test_id)
    except Exception as e:
        click.echo(f"Fails to delete test result {test_id}.\nException: {e}")
        sys.exit(1)


if __name__ == "__main__":
    cli()
