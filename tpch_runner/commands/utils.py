import shutil
import sys
import textwrap
import warnings
from datetime import datetime
from typing import Optional, Type

import matplotlib.pyplot as plt
import numpy as np

from .. import meta
from ..tpch.databases import base

warnings.filterwarnings(
    "ignore", category=UserWarning, module="tpch_runner.commands.utils"
)


def custom_warning_handler(message, category, filename, lineno, file=None, line=None):
    print(f"WARNING from {filename}:{lineno} - {category.__name__}: {message}")


warnings.showwarning = custom_warning_handler


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
        print("Either database ID or alias is required.", file=sys.stderr)
        sys.exit(1)

    try:
        db: meta.Database = rm.get_databases(id=id, alias=alias_)[0]
        if not db:
            print(f"Database {id} or alias {alias_} not found.", file=sys.stderr)
            sys.exit(1)
        elif db.db_type not in meta.db_classes:
            print(f"Unsupported database type: {db.db_type}", file=sys.stderr)
            sys.exit(1)
        return db
    except IndexError:
        db_name = alias_ if alias_ else str(id)
        print(f"Error fetching database: {db_name}", file=sys.stderr)
        sys.exit(1)


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
    elif db.db_type == "rapidsdb":
        from ..tpch.databases.rapidsdb import RDP_TPCH, RapidsDB

        db_class = RDP_TPCH
        conn_class = RapidsDB
    elif db.db_type == "duckdb":
        from ..tpch.databases.duckdb import Duckdb_TPCH, DuckLDB

        db_class = Duckdb_TPCH
        conn_class = DuckLDB
    else:
        raise ValueError(f"Unsupported database type: {db.db_type}")

    if db.db_type == "duckdb":
        dbconn = conn_class(
            host=db.host,
            port=db.port,
            db_name=db.dbname,
            user=db.user,
            password=db.password,
        )
    else:
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


def barchart(title, data, fpath):
    labels = [f"Q{i}" for i in range(1, 23)]
    if not data or len(data) != 22:
        raise ValueError("data can't be empty and must have 22 elements.")

    plt.figure(figsize=(12, 6))
    plt.bar(labels, data, color="skyblue")

    plt.title(f"{title} TPC-H Queries Runtime", fontsize=16)
    plt.xlabel("Query", fontsize=14)
    plt.ylabel("Runtime (seconds)", fontsize=14)
    plt.xticks(rotation=45)
    plt.tight_layout()
    plt.legend()
    plt.savefig(fpath, dpi=300)


def barchart2(
    d1_label,
    d2_label,
    data1: list[float],
    data2: list[float],
    fpath: str,
):
    labels = [f"Q{i}" for i in range(1, 23)]

    if not data1 or len(data1) != 22:
        raise ValueError("data1 can't be empty and must have 22 elements.")
    elif not data2 or len(data2) != 22:
        raise ValueError("data2 can't be empty and must have 22 elements.")

    x = np.arange(len(labels))  # the label locations
    width = 0.35  # the width of the bars

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(x - width / 2, data1, width, label=d1_label, color="blue")
    ax.bar(x + width / 2, data2, width, label=d2_label, color="orange")

    ax.set_xlabel("Queries")
    ax.set_ylabel("Execution Time (s)")
    ax.set_title("Comparison of TPC-H Power Test Records")
    ax.set_xticks(x)
    ax.set_xticklabels(labels)
    ax.legend()

    # Show bar chart
    plt.tight_layout()
    plt.savefig(f"{fpath}.png", dpi=300)


def linechart(title, data, fpath):
    labels = [f"Q{i}" for i in range(1, 23)]

    plt.figure(figsize=(12, 6))
    plt.plot(labels, data, marker="o", linestyle="-", color="blue")

    plt.title(f"{title} TPC-H Queries Runtime", fontsize=16)
    plt.xlabel("Query", fontsize=14)
    plt.ylabel("Runtime (seconds)", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend()
    plt.savefig(fpath, dpi=300)


def linechart_multi(trends: list[dict], fpath: str) -> None:
    """
    Generate a line chart to visualize TPC-H query runtime trends.

    Parameters:
    - trends (list[dict]): A list of dictionaries, each containing:
        - 'name': A string representing the trend name.
        - 'data': A list of y-axis values corresponding to labels.
    - fpath (str): The file path to save the generated chart.
    """
    labels = [f"Q{i}" for i in range(1, 23)]

    plt.figure(figsize=(12, 6))
    for trend in trends:
        name = trend.get("name", None)
        data = trend.get("data", [])
        plt.plot(labels, data, marker="o", linestyle="-", label=name)

    plt.title("TPC-H Queries Runtime", fontsize=16)
    plt.xlabel("Query", fontsize=14)
    plt.ylabel("Runtime (seconds)", fontsize=14)
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.legend()
    plt.savefig(fpath, dpi=300)


def barchart_multi(trends: list[dict], fpath: str) -> None:
    """
    Generate a barchart to compare total runtimes of multiple TPC-H powertests.

    Parameters:
    - trends (list[dict]): A list of dictionaries, each containing:
        - 'name': A string representing the trend name.
        - 'data': A list of y-axis values corresponding to labels.
    - fpath (str): The file path to save the generated chart.
    """
    labels = ["total runtime"]

    # num_trends = len(trends)
    indices = np.arange(1)
    fig, ax = plt.subplots(figsize=(10, 6))
    bar_width = 0.15

    for i, trend in enumerate(trends):
        name = trend.get("name", None)
        data = trend.get("data", [])
        ax.bar(indices + i * bar_width, data, width=bar_width, label=name)

    # ax.set_xlabel("Tests")
    ax.set_ylabel("Total Time (s)")
    ax.set_title("Comparison of TPC-H PowerTest Runtime")

    ax.set_xticks(indices + (bar_width * (len(trends) - 1)) / 2)
    ax.set_xticklabels(labels)
    ax.legend()

    plt.tight_layout()
    plt.savefig(f"{fpath}.png", dpi=300)


def linechart2(
    d1_label,
    d2_label,
    data1: list[float],
    data2: list[float],
    fpath: str,
):
    labels = [f"Q{i}" for i in range(1, 23)]

    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(labels, data1, marker="o", label=d1_label, color="blue")
    ax.plot(labels, data2, marker="s", label=d2_label, color="orange")

    ax.set_xlabel("Queries")
    ax.set_ylabel("Execution Time (s)")
    ax.set_title("Comparison of TPC-H Power Test Records")
    ax.legend()

    plt.tight_layout()
    plt.savefig(f"{fpath}.png", dpi=300)


def wrap_column(column_text) -> str:
    """Dynamically set column width and return column text that is adjusted to width."""
    termina_width = shutil.get_terminal_size((80, 20)).columns
    column_width = max(30, termina_width // 3)
    if len(column_text) > column_width:
        return "\n".join(textwrap.wrap(column_text, width=column_width))
    return column_text
