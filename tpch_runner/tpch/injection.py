#!/usr/bin/env python
import asyncio
import csv
import json
import random
import subprocess
import sys
from pathlib import Path
from typing import AsyncGenerator, Generator, Optional

from tpch_runner import config

RANDOM_LAG = config.random_lag or False
MAX_LAG_TIME = config.max_lag_time or 1
LAG_INTERVAL = config.lag_interval or 0

ENV_VARS = {
    "DSS_PATH": "data",
    "DSS_CONFIG": "tool",
    "DSS_DIST": "dists.dss",
    "DSS_QUERY": "templates",
}

TABLE_MAP = {
    "customer": "c",
    "region": "r",
    "nation": "n",
    "lineitem": "L",
    "orders": "O",
    "parts": "P",
    "parsupp": "S",
    "suppliers": "s",
}


def data_gen_batch(
    table: str, sf: int, env_vars: Optional[dict] = None
) -> tuple[bool, str]:
    if not env_vars:
        env_vars = ENV_VARS
    if "data_dir" in dir(config):
        data_dir = Path(config.data_dir)
    else:
        data_dir = Path(env_vars["DSS_PATH"])
    data_dir = data_dir.expanduser().joinpath("sf" + str(sf))
    data_dir.mkdir(exist_ok=True)
    env_vars["DSS_PATH"] = str(data_dir)

    table_key = TABLE_MAP.get(table)
    command = f"./tool/dbgen -T {table_key} -f -s {sf}"
    cwd = Path(__file__).parent.as_posix()
    result = subprocess.run(
        command,
        env=env_vars,
        capture_output=True,
        text=True,
        shell=True,
        cwd=cwd,
    )
    if result.returncode == 0:
        print(
            f"succeeds, output: {result.stdout.splitlines() if result.stdout else 'done'}"
        )
        return True, result.stdout
    else:
        print(f"dbgen fails, error: {result.stderr.splitlines()}")
        return False, result.stderr


def generate_data(table: str, sf: int = 1, cwd: Optional[str] = None) -> Generator:
    """
    Runs the tool and yields raw CSV data line by line.
    """
    command = f"./dbgen -T {table} -s {sf} -z"
    process = subprocess.Popen(
        command,
        env=ENV_VARS,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=cwd,
        shell=True,
    )

    # Yield each line of the raw CSV output
    for line in iter(process.stdout.readline, ""):  # type: ignore
        yield line.strip()

    # Close the process' stdout and wait for the process to finish
    process.stdout.close()  # type: ignore
    process.wait()


async def csv_to_json(
    raw_csv_generator: Generator, fieldnames: list[str]
) -> AsyncGenerator:
    """Endpoint to stream region table data."""
    csv_reader = csv.DictReader(
        raw_csv_generator,
        fieldnames=fieldnames,
        delimiter="|",
    )
    for row in csv_reader:
        try:
            del row[None]
        except Exception:
            pass
        if RANDOM_LAG:
            lagtime = random.uniform(0, MAX_LAG_TIME)
        else:
            lagtime = LAG_INTERVAL
        row["lag_time"] = round(lagtime, 2)

        yield json.dumps(row) + "\n"

        await asyncio.sleep(lagtime)


def main(args):
    action = args[1]
    try:
        if action == "dbgen":
            print("run dbgen")
            table = args[2]
            if table not in TABLE_MAP.keys():
                raise ValueError(
                    "talbe name not right, input one from \n{}.".format(
                        ", ".join(TABLE_MAP.keys())
                    )
                )
            table_key = TABLE_MAP.get(table)
            scale_factor = args[3] if len(args) == 4 else 1
            data_gen_batch(table_key, scale_factor)
        else:
            print("run qgen")
    except Exception as e:
        print(f"Action fails, exception: {e}")


if __name__ == "__main__":
    main(sys.argv)
