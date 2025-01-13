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

env_vars = {
    "DSS_PATH": "data",
    "DSS_CONFIG": "tool",
    "DSS_DIST": "dists.dss",
    "DSS_QUERY": "templates",
}

table_map = {
    "customer": "c",
    "region": "r",
    "nation": "n",
    "lineitem": "L",
    "orders": "o",
    "parts": "P",
    "parsupp": "S",
    "suppliers": "s",
}


def data_gen_batch(table: str, sf: int):
    command = f"./tool/dbgen -T {table} -f -s {sf}"
    cwd = Path(__file__).parent.as_posix()
    # print("full command:", command, "cwd:", cwd, type(cwd))
    result = subprocess.run(
        command,
        env=env_vars,
        capture_output=True,
        text=True,
        shell=True,
        cwd=cwd,
    )
    if result.returncode == 0:
        print(f"succeeds, output: {result.stdout.splitlines()}")
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
        env=env_vars,
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


async def stream_csv_to_json(file_path: str, fieldnames: list[str]) -> AsyncGenerator:
    """Generator function to stream a CSV file as JSON."""
    with open(file_path, mode="r", encoding="utf-8") as csv_file:
        reader = csv.DictReader(
            csv_file,
            fieldnames=fieldnames,
            delimiter="|",
        )

        for row in reader:
            # delete empty column inferred by line terminator if exists
            row.pop(None, None)

            if RANDOM_LAG:
                lagtime = random.uniform(0, MAX_LAG_TIME)
            else:
                lagtime = LAG_INTERVAL
            row["lag_time"] = round(lagtime, 2)

            json_str = json.dumps(row) + "\n"

            yield json_str.encode("utf-8")

            await asyncio.sleep(lagtime)


def main(args):
    # print("args:", args)
    action = args[1]
    try:
        if action == "dbgen":
            print("run dbgen")
            table = args[2]
            if table not in table_map.keys():
                raise ValueError(
                    "talbe name not right, input one from \n{}.".format(
                        ", ".join(table_map.keys())
                    )
                )
            table_key = table_map.get(table)
            scale_factor = args[3] if len(args) == 4 else 1
            data_gen_batch(table_key, scale_factor)
        else:
            print("run qgen")
    except Exception as e:
        print(f"Action fails, exception: {e}")


if __name__ == "__main__":
    # if len(sys.argv) != 2 or sys.argv[1] not in ["pyrdp", "aiordp"]:
    #     print("wrong arguments")
    main(sys.argv)
