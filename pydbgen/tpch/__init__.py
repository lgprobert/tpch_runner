# flake8: noqa: F401
import time
from collections import namedtuple
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Any

import pandas as pd

Result = namedtuple("Result", ["db", "idx", "rowcount", "rset", "columns", "result_dir"])

DATA_DIR = Path("~/data/tpch/small").expanduser()
RESULT_DIR = Path("~/data/result").expanduser()

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
        runtime = round((end_time - start_time), 2)
        print(f"{runtime:.2f} seconds.")
        # return result, runtime

        if isinstance(results, tuple):
            return (*results, runtime)
        else:
            return results, runtime

    return wrapper


def post_process(func):
    @wraps(func)
    def wrapper(*args, **kwargs) -> Any:
        func_name = func.__wrapped__.__name__
        if func_name == "run_query":
            _results = func(*args, **kwargs)
            print(f"Processing result: {_results}")
            db_type, idx, rowcount, rset, columns, result_dir, runtime = _results
            df = pd.DataFrame(rset, columns=columns)
            current_timestamp = datetime.now().strftime("%Y-%m-%d-%H%M%S")
            csv_file_name = "{}_{}_{}.csv".format(db_type, f"q{idx}", current_timestamp)

            if result_dir:
                df.to_csv(result_dir.joinpath(csv_file_name), index=False)
            else:
                df.to_csv(RESULT_DIR.joinpath(csv_file_name), index=False)
            return rowcount, rset, columns, runtime
        return _results

    return wrapper
