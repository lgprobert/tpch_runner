from pathlib import Path
from typing import Any, Optional

import click
from rich_click import RichGroup
from tabulate import tabulate

from tpch_runner.config import app_root

from .. import meta
from . import CONTEXT_SETTINGS
from .utils import barchart, barchart2, format_datetime, linechart, linechart2


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


@cli.command("delete")
@click.argument("test_id")
@click.pass_obj
def delete(ctx, test_id: int):
    """Delete a Powertest record.

    TEST_ID: ID of the test to delete.
    """
    rm: meta.TestResultManager = ctx["rm"]

    try:
        rm.delete_powertest(test_id)
    except Exception as e:
        click.echo(f"Fails to delete Powertest result {test_id}.\nException: {e}")
        return


@cli.command("update")
@click.argument("test_id")
@click.option("-c", "--comment", help="Test comment")
@click.option("-s", "--scale", help="Data scale")
@click.pass_obj
def update(ctx, test_id: int, comment: Optional[str] = None, scale: Optional[str] = None):
    """Update a Powertest record.

    TEST_ID: ID of the test to update.
    """
    rm: meta.TestResultManager = ctx["rm"]

    try:
        rm.update_powertest_comment(
            test_id=test_id,
            comment=comment,
            scale=scale,
        )
    except Exception as e:
        click.echo(f"Fails to update Powertest result {test_id}.\nException: {e}")
        return


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
    """List two Powertest results."""
    rm: meta.TestResultManager = ctx["rm"]

    src_result: meta.PowerTest = rm.get_powertests(test_id=source)[0]
    dest_result: meta.PowerTest = rm.get_powertests(test_id=dest)[0]

    report: list[tuple] = []
    report.append(("Database", src_result.db_type, dest_result.db_type))
    report.append(("Scale", src_result.scale, dest_result.scale))
    report.append(("Success", src_result.success, dest_result.success))
    report.append(
        ("Runtime (s)", f"{src_result.runtime: .4f}", f"{dest_result.runtime: .4f}")
    )
    report.append(("Result Folder", src_result.result_folder, dest_result.result_folder))

    src_query_results: list[meta.TestResult] = src_result.results
    dest_query_results: list[meta.TestResult] = dest_result.results
    query_reports: list[tuple] = []
    for rec1, rec2 in zip(src_query_results, dest_query_results):
        query_reports.append(
            (
                rec1.query_name,
                rec1.success,
                rec2.success,
                rec1.rowcount,
                rec2.rowcount,
                f"{rec1.runtime: .4f}",
                f"{rec2.runtime: .4f}",
            )
        )

    print("\nPowertest Result Comparison:")
    print(
        tabulate(report, headers=["Attribute", "Source", "Destination"], tablefmt="psql")
    )
    print("\n" + "-" * 70)
    print("\nPowertest Individual Query Result Comparison:")
    print(
        tabulate(
            query_reports,
            headers=[
                "Query",
                "Success-1",
                "Success-2",
                "Rowcount-1",
                "Rowcount-2",
                "Runtime (s)-1",
                "Runtime (s)-2",
            ],
            tablefmt="psql",
        )
    )


@cli.command("show")
@click.argument("test_id")
@click.pass_obj
def show(ctx, test_id: int):
    """Show Powertest result details.

    TEST_ID: ID of the test to show.
    """
    rm: meta.TestResultManager = ctx["rm"]

    try:
        result: meta.PowerTest = rm.get_powertests(test_id=test_id)[0]
        report = []
        result_detail: dict[str, Any] = {}
        result_detail["ID"] = result.id
        result_detail["Database"] = result.db_type
        result_detail["Scale"] = result.scale
        result_detail["Test Time"] = format_datetime(result.testtime)  # type: ignore
        result_detail["Success"] = result.success
        result_detail["Runtime (s)"] = result.runtime
        result_detail["Result Folder"] = result.result_folder
        for k, v in result_detail.items():
            report.append((k, v))

        # get query results from powertest
        query_results: list[meta.TestResult] = result.results
        query_reports = []
        for query in query_results:
            query_reports.append(
                (
                    query.id,
                    query.query_name,
                    query.success,
                    query.rowcount,
                    query.runtime,
                    query.result_csv,
                )
            )
        print("\nPowertest Details:")
        print(tabulate(report, headers=["Attribute", "Value", "Value"], tablefmt="psql"))
        print("\n" + "-" * 70)
        print("\nPowertest Individual Query Results:")
        print(
            tabulate(
                query_reports,
                headers=[
                    "ID",
                    "Query",
                    "Success",
                    "Rowcount",
                    "Runtime (s)",
                    "Result CSV",
                ],
                tablefmt="psql",
            )
        )
    except Exception as e:
        click.echo(f"Fails to show details of Powertest {test_id}.\nException: {e}")
        return


@cli.command("draw")
@click.option(
    "-c",
    "--chart",
    type=click.Choice(["bar", "line"]),
    default="bar",
    help="Chart type",
)
@click.argument("test_id")
@click.argument("ref_test_id")
@click.pass_obj
def draw(ctx, test_id: int, ref_test_id: int, chart: str) -> None:
    """Generate Powertest runtime chart.

    TEST_ID: ID of the test to show.
    REF_TEST_ID: ID of the reference test to compare with (optional).
    """
    rm: meta.TestResultManager = ctx["rm"]
    compare: bool = False

    chart_functions = {
        "bar": (barchart, barchart2),
        "line": (linechart, linechart2),
    }

    single_chart_func, compare_chart_func = chart_functions[chart]

    if chart not in chart_functions:
        raise ValueError(f"Unsupported chart type: {chart}")

    try:
        db, test_name, total_runtime, query_runtimes = rm.get_powertest_runtime(test_id)
        chart_file_path = Path(app_root).joinpath(f"{test_name}.png").expanduser()
        if ref_test_id:
            compare = True
            ref_db, ref_test_name, _, ref_query_runtimes = rm.get_powertest_runtime(
                ref_test_id
            )
            print(f"Comparing {test_name} with {ref_test_name}")
            chart_file_path = (
                Path(app_root)
                .joinpath(f"{test_name}-{ref_test_name}_{chart}chart.png")
                .expanduser()
            )

        queries = [f"Q{i}" for i in range(1, 23)]
    except Exception as e:
        click.echo(f"Fails to retrieve Powertest {test_id} record.\nException: {e}")
        return

    # generate chart
    if compare:
        compare_chart_func(
            db, ref_db, queries, query_runtimes, ref_query_runtimes, str(chart_file_path)
        )
    else:
        single_chart_func(db, queries, query_runtimes, chart_file_path)
    print(f"Chart saved to {chart_file_path}")


if __name__ == "__main__":
    cli()
