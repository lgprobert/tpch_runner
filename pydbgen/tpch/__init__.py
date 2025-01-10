# flake8: noqa: F401
import time
from functools import wraps
from pathlib import Path

DATA_DIR = Path("~/data/tpch/small").expanduser()

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
        result = func(*args, **kwargs)
        end_time = time.time()
        runtime = round((end_time - start_time), 2)
        print(f"{runtime:.2f} seconds.")
        # return result, runtime

        if isinstance(result, tuple):
            return (*result, runtime)
        else:
            return result, runtime

    return wrapper
