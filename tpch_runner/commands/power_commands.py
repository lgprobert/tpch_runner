from pathlib import Path
from typing import Any

import click
import matplotlib.pyplot as plt
from rich_click import RichGroup
from tabulate import tabulate

from tpch_runner.config import app_root

from ..tpch.databases import meta
from . import CONTEXT_SETTINGS, format_datetime


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
@click.pass_obj
def draw(ctx, test_id: int, chart: str) -> None:
    """Generate Powertest runtime chart.

    TEST_ID: ID of the test to show.
    """
    rm: meta.TestResultManager = ctx["rm"]

    try:
        db, test_name, total_runtime, query_runtimes = rm.get_powertest_runtime(test_id)
        chart_file_path = Path(app_root).joinpath(f"{test_name}.png").expanduser()
        queries = [f"Q{i}" for i in range(1, 23)]

        if chart == "bar":
            barchart(db, queries, query_runtimes, chart_file_path)
        else:
            linechart(db, queries, query_runtimes, chart_file_path)
        print(f"Chart saved to {chart_file_path}")
    except Exception as e:
        click.echo(f"Fails to retrieve Powertest {test_id} record.\nException: {e}")
        return


def barchart(title, labels, data, fpath):
    plt.figure(figsize=(12, 6))
    plt.bar(labels, data, color="skyblue")

    plt.title(f"{title} TPC-H Queries Runtime", fontsize=16)
    plt.xlabel("Query", fontsize=14)
    plt.ylabel("Runtime (seconds)", fontsize=14)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(fpath, dpi=300)


def linechart(title, labels, data, fpath):
    plt.figure(figsize=(12, 6))
    plt.plot(labels, data, marker="o", linestyle="-", color="blue")

    plt.title(f"{title} TPC-H Queries Runtime", fontsize=16)
    plt.xlabel("Query", fontsize=14)
    plt.ylabel("Runtime (seconds)", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.savefig(fpath, dpi=300)


if __name__ == "__main__":
    cli()
