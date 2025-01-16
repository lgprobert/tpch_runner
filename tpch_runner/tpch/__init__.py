# flake8: noqa: F401
import time
from collections import namedtuple
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any

import pandas as pd

from tpch_runner.config import app_root

Result = namedtuple(
    "Result",
    ["db", "idx", "success", "rowcount", "rset", "columns", "result_dir", "metadb"],
)

DATA_DIR = Path("~/data/tpch/small").expanduser()
RESULT_DIR = Path(app_root).expanduser().joinpath("results")
ANSWER_DIR = Path(__file__).parent.joinpath("answer")

DATA_DIR.mkdir(exist_ok=True)
RESULT_DIR.mkdir(exist_ok=True)


all_tables = [
    "region",
    "nation",
    "customer",
    "part",
    "supplier",
    "partsupp",
    "orders",
    "lineitem",
]


def timeit(func):
    """Decorator to time function execution time."""

    @wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        results = func(*args, **kwargs)
        end_time = time.time()
        runtime = round((end_time - start_time), 4)
        print(f"{runtime:.4f} seconds.")

        if isinstance(results, tuple):
            return (*results, runtime)
        else:
            return results, runtime

    return wrapper


def post_process(func):
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        from .databases.meta import TestResultManager

        metadb: TestResultManager
        result_dir: Path

        func_name = func.__wrapped__.__name__
        if func_name == "run_query":
            # breakpoint()
            _results, no_report, runtime = func(*args, **kwargs)
            (
                db_type,
                idx,
                success,
                rowcount,
                rset,
                columns,
                result_dir,
                metadb,
            ) = _results

            if not no_report:
                df = pd.DataFrame(rset, columns=columns)
                current_timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
                csv_file_name = "{}_{}_{}.csv".format(
                    db_type, f"q{idx}", current_timestamp
                )

                if result_dir:
                    csv_file_name = f"{idx}.csv"
                    df.to_csv(Path(result_dir).joinpath(csv_file_name), index=False)
                else:
                    df.to_csv(RESULT_DIR.joinpath(csv_file_name), index=False)
                result_folder = str(result_dir.stem) if result_dir else None
                metadb.add_test_result(
                    db_type=db_type,
                    success=success,
                    rowcount=rowcount,
                    result_csv=csv_file_name,
                    query_name=idx,
                    runtime=runtime,
                    result_folder=result_folder,
                )
            return success, rowcount, rset, columns, runtime
        return _results

    return wrapper
