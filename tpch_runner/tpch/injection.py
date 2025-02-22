#!/usr/bin/env python
import subprocess
import sys
from pathlib import Path
from typing import Optional

from tpch_runner.config import Config

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
        data_dir = Path(Config.data_dir)
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
        return True, result.stdout
    return False, result.stderr


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
