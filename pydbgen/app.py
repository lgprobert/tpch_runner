import asyncio
import csv
import json
import random

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

from . import config

app = FastAPI()

RANDOM_LAG = config.random_lag or False
MAX_LAG_TIME = config.max_lag_time or 1
LAG_INTERVAL = config.lag_interval or 0

# CSV_FILE_PATH = "/software/data/tpch/sf1"
CSV_FILE_PATH = "pydbgen/tpch/data"


async def stream_csv_to_json(file_path: str, fieldnames: list):
    """Generator function to stream a CSV file as JSON."""
    try:
        with open(file_path, mode="r", encoding="utf-8") as csv_file:
            reader = csv.DictReader(
                csv_file,
                fieldnames=fieldnames,
                delimiter="|",
            )
            for row in reader:
                # delete empty column inferred by line terminator if exists, avoid if
                try:
                    del row[None]
                except Exception:
                    pass
                if RANDOM_LAG:
                    lagtime = random.uniform(0, MAX_LAG_TIME)
                else:
                    lagtime = LAG_INTERVAL
                row["lag_time"] = round(lagtime, 2)

                json_str = json.dumps(row) + "\n"
                yield json_str.encode("utf-8")

                await asyncio.sleep(lagtime)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="CSV file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file: {e}")


@app.get("/region", response_class=StreamingResponse)
async def get_region():
    """Endpoint to stream region table data."""
    generator = stream_csv_to_json(
        f"{CSV_FILE_PATH}/region.tbl", ["r_regionkey", "r_name", "r_comment"]
    )
    return StreamingResponse(generator, media_type="application/json")


@app.get("/nation", response_class=StreamingResponse)
async def get_nation():
    """Endpoint to stream nation table data."""
    generator = stream_csv_to_json(
        f"{CSV_FILE_PATH}/nation.tbl",
        ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    )
    return StreamingResponse(generator, media_type="application/json")


@app.get("/customer", response_class=StreamingResponse)
async def get_customer():
    """Endpoint to stream customer table data."""
    generator = stream_csv_to_json(
        f"{CSV_FILE_PATH}/customer.tbl",
        [
            "c_custkey",
            "c_name",
            "c_address",
            "c_nationkey",
            "c_phone",
            "c_acctbal",
            "c_mktsegment",
            "c_comment",
        ],
    )
    return StreamingResponse(generator, media_type="application/json")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
