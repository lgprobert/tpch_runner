from pathlib import Path
from typing import AsyncGenerator

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse

from tpch_runner import config
from tpch_runner.tpch.injection import csv_to_json, generate_data, stream_csv_to_json

app = FastAPI()

RANDOM_LAG = config.random_lag or False
MAX_LAG_TIME = config.max_lag_time or 1
LAG_INTERVAL = config.lag_interval or 0

# CSV_FILE_PATH = "/software/data/tpch/sf1"
CSV_FILE_PATH = "tpch_runner/tpch/data"
TPCH_DIR = Path(__file__).parent.joinpath("tpch")

table_fields = {
    "region": ["r_regionkey", "r_name", "r_comment"],
    "nation": ["n_nationkey", "n_name", "n_regionkey", "n_comment"],
    "customer": [
        "c_custkey",
        "c_name",
        "c_address",
        "c_nationkey",
        "c_phone",
        "c_acctbal",
        "c_mktsegment",
        "c_comment",
    ],
}


async def stream_csv_file_to_json(table: str) -> AsyncGenerator:
    """Generator function to stream a CSV file as JSON."""
    try:
        if table not in table_fields.keys():
            raise ValueError(f"Table name {table} not valid.")
        file_path = f"{TPCH_DIR}/data/{table}.tbl"
        fieldnames = table_fields.get(table)
        async for json_str in stream_csv_to_json(file_path, fieldnames):  # type: ignore
            yield json_str
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="CSV file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading file: {e}")


async def get_table_generator(table: str) -> AsyncGenerator:
    raw_csv_generator = generate_data(table, cwd=str(TPCH_DIR))
    # calling async function that return AsyncGenerator do not need 'await'
    return csv_to_json(raw_csv_generator, table_fields.get(table))  # type: ignore


@app.get("/file/region", response_class=StreamingResponse)
async def get_region_file():
    """Endpoint to stream region table data."""
    json_generator = stream_csv_file_to_json("region")
    return StreamingResponse(json_generator, media_type="application/json")


@app.get("/file/nation", response_class=StreamingResponse)
async def get_nation_file():
    """Endpoint to stream nation table data."""
    json_generator = stream_csv_file_to_json("nation")
    return StreamingResponse(json_generator, media_type="application/json")


# @app.get("/region", response_class=StreamingResponse)
# async def get_region():
#     """Endpoint to stream region table data."""
#     generator = stream_csv_to_json(
#         f"{CSV_FILE_PATH}/region.tbl", ["r_regionkey", "r_name", "r_comment"]
#     )
#     return StreamingResponse(generator, media_type="application/json")


@app.get("/region", response_class=StreamingResponse)
async def get_region():
    """Endpoint to stream region table data."""
    json_generator = await get_table_generator("region")
    return StreamingResponse(json_generator, media_type="application/json")


@app.get("/nation", response_class=StreamingResponse)
async def get_nation():
    """Endpoint to stream nation table data."""
    json_generator = await get_table_generator("nation")
    return StreamingResponse(json_generator, media_type="application/json")


@app.get("/customer", response_class=StreamingResponse)
async def get_customer():
    """Endpoint to stream customer table data."""
    # generator = stream_csv_to_json(
    #     f"{CSV_FILE_PATH}/customer.tbl",
    #     [
    #         "c_custkey",
    #         "c_name",
    #         "c_address",
    #         "c_nationkey",
    #         "c_phone",
    #         "c_acctbal",
    #         "c_mktsegment",
    #         "c_comment",
    #     ],
    # )
    # return StreamingResponse(generator, media_type="application/json")

    json_generator = await get_table_generator("customer")
    return StreamingResponse(json_generator, media_type="application/json")


def main():
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
