import sys
from datetime import datetime
from typing import Optional, Type

import click
import matplotlib.pyplot as plt

from .. import meta
from ..tpch.databases import base


def format_datetime(atime: datetime) -> str:
    if atime.date() == datetime.now().date():
        fmt_datetime_value = atime.strftime("%H:%M:%S")
    else:
        fmt_datetime_value = atime.strftime("%Y-%m-%d")

    return fmt_datetime_value


def get_db(
    rm: meta.DBManager, id: Optional[int] = None, alias_: Optional[str] = None
) -> meta.Database:
    if not id and not alias_:
        click.echo("Either database ID or alias is required.")
        sys.exit(1)
    db: meta.Database = rm.get_databases(id=id, alias=alias_)[0]
    if not db:
        click.echo(f"Database {id} or alias {alias_} not found.")
        sys.exit(1)
    elif db.db_type not in meta.db_classes:
        click.echo(f"Unsupported database type: {db.db_type}")
        sys.exit(1)
    return db


def get_db_manager(db: meta.Database, scale: str = "small") -> base.TPCH_Runner:
    conn_class: Type[base.Connection]
    db_class: Type[base.TPCH_Runner]
    if db.db_type == "mysql":
        from ..tpch.databases.mysqldb import MySQL_TPCH, MySQLDB

        db_class = MySQL_TPCH
        conn_class = MySQLDB
    elif db.db_type == "pg":
        from ..tpch.databases.pgdb import PG_TPCH, PGDB

        db_class = PG_TPCH
        conn_class = PGDB
    else:
        raise ValueError(f"Unsupported database type: {db.db_type}")

    dbconn = conn_class(
        host=db.host,
        port=int(db.port),
        db_name=db.dbname,
        user=db.user,
        password=db.password,
    )
    db_manager: base.TPCH_Runner = db_class(
        dbconn, db_id=db.id, scale=scale  # type: ignore
    )
    return db_manager


def barchart(title, labels, data, fpath):
    plt.figure(figsize=(12, 6))
    plt.bar(labels, data, color="skyblue")

    plt.title(f"{title} TPC-H Queries Runtime", fontsize=16)
    plt.xlabel("Query", fontsize=14)
    plt.ylabel("Runtime (seconds)", fontsize=14)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.savefig(fpath, dpi=300)


def barchart2(
    d1_label,
    d2_label,
    labels: list[str],
    data1: list[float],
    data2: list[float],
    fpath: str,
):
    import matplotlib.pyplot as plt
    import numpy as np

    # Data
    queries = labels

    # Bar Chart Setup
    x = np.arange(len(queries))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(x - width / 2, data1, width, label=d1_label, color="blue")
    ax.bar(x + width / 2, data2, width, label=d2_label, color="orange")

    # Add labels, title, and legend
    ax.set_xlabel("Queries")
    ax.set_ylabel("Execution Time (s)")
    ax.set_title("Comparison of TPC-H Power Test Records")
    ax.set_xticks(x)
    ax.set_xticklabels(queries)
    ax.legend()

    # Show bar chart
    plt.tight_layout()
    plt.savefig(f"{fpath}.png", dpi=300)


def linechart(title, labels, data, fpath):
    plt.figure(figsize=(12, 6))
    plt.plot(labels, data, marker="o", linestyle="-", color="blue")

    plt.title(f"{title} TPC-H Queries Runtime", fontsize=16)
    plt.xlabel("Query", fontsize=14)
    plt.ylabel("Runtime (seconds)", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.savefig(fpath, dpi=300)


def linechart_multi(
    title: str, labels: list[str], trends: list[dict], fpath: str
) -> None:
    """
    Generate a line chart to visualize TPC-H query runtime trends.

    Parameters:
    - title (str): The title of the chart.
    - labels (list[str]): The labels for the x-axis.
    - trends (list[dict]): A list of dictionaries, each containing:
        - 'name': A string representing the trend name.
        - 'data': A list of y-axis values corresponding to labels.
    - fpath (str): The file path to save the generated chart.

    Returns:
    None
    """
    plt.figure(figsize=(12, 6))
    for trend in trends:
        name = trend.get("name", None)
        data = trend.get("data", [])
        plt.plot(labels, data, marker="o", linestyle="-", label=name)

    plt.title(f"{title} TPC-H Queries Runtime", fontsize=16)
    plt.xlabel("Query", fontsize=14)
    plt.ylabel("Runtime (seconds)", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.savefig(fpath, dpi=300)


def linechart2(
    d1_label,
    d2_label,
    labels: list[str],
    data1: list[float],
    data2: list[float],
    fpath: str,
):
    # Line Chart Setup
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(labels, data1, marker="o", label=d1_label, color="blue")
    ax.plot(labels, data2, marker="s", label=d2_label, color="orange")

    # Add labels, title, and legend
    ax.set_xlabel("Queries")
    ax.set_ylabel("Execution Time (s)")
    ax.set_title("Comparison of TPC-H Power Test Records")
    ax.legend()

    # Show line chart
    plt.tight_layout()
    plt.savefig(f"{fpath}.png", dpi=300)
