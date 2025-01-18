# flake8: noqa: F401
import time
from collections import namedtuple
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any, NamedTuple, Optional

import pandas as pd

from tpch_runner.config import app_root

Result = namedtuple(
    "Result",
    ["success", "rowcount", "rset", "columns", "result_file"],
)

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


class InternalQueryArgs(NamedTuple):
    db: str
    no_report: bool
    idx: int
    result_dir: Optional[Path]
    metadb: Any
    db_id: int


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

        if isinstance(results, tuple) and func.__name__ != "run_query":
            return (*results, runtime)
        elif isinstance(results, tuple) and func.__name__ == "run_query":
            return results[0], runtime, results[2]
        else:
            return results, runtime

    return wrapper


def post_process(func):
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        from ..meta import TestResultManager

        metadb: TestResultManager
        _args: InternalQueryArgs

        func_name = func.__wrapped__.__name__
        if func_name == "run_query":
            _results, runtime, _args = func(*args, **kwargs)
            success, rowcount, rset, columns, _ = _results
            metadb = _args.metadb

            csv_file_name: Optional[str] = None
            if not _args.no_report:
                df = pd.DataFrame(rset, columns=columns)
                current_timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
                csv_file_name = "{}_{}_{}.csv".format(
                    _args.db, f"q{_args.idx}", current_timestamp
                )

                if _args.result_dir is not None:
                    csv_file_name = f"{_args.idx}.csv"
                    df.to_csv(_args.result_dir.joinpath(csv_file_name), index=False)
                else:
                    df.to_csv(RESULT_DIR.joinpath(csv_file_name), index=False)
                result_folder = str(_args.result_dir.stem) if _args.result_dir else None
                metadb.add_test_result(
                    db_type=_args.db,
                    success=success,
                    rowcount=rowcount,
                    result_csv=csv_file_name,
                    query_name=_args.idx,
                    runtime=runtime,
                    result_folder=result_folder,
                    db_id=_args.db_id,
                )
            return Result(success, rowcount, rset, columns, csv_file_name), runtime, None
        return _results

    return wrapper
