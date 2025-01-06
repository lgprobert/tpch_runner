#!/usr/bin/env python
import subprocess
import sys

env_vars = {
    "DSS_PATH": "./data",
    "DSS_CONFIG": ".",
    "DSS_DIST": "dists.dss",
    "DSS_QUERY": "./templates",
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


def data_gen(table: str, sf: int):
    # dbgen_args = ["-T", table, "-s", sf]
    # command = "ls . && pwd"
    # command = ["./dbgen"] + dbgen_args
    command = f"./dbgen -T {table} -s {sf}"
    print("full command:", command)
    result = subprocess.run(
        command, env=env_vars, capture_output=True, text=True, shell=True, timeout=60
    )
    if result.returncode == 0:
        print(f"succeeds, output: {result.stdout.splitlines()}")
        return True, result.stdout
    else:
        print(f"dbgen fails, error: {result.stderr.splitlines()}")
        return False, result.stderr


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
            data_gen(table_key, scale_factor)
        else:
            print("run qgen")
    except Exception as e:
        print(f"Action fails, exception: {e}")


if __name__ == "__main__":
    # if len(sys.argv) != 2 or sys.argv[1] not in ["pyrdp", "aiordp"]:
    #     print("wrong arguments")
    main(sys.argv)
