"""Module for MySQL database TPC-H benchmark runner."""

import logging
import sys
from pathlib import Path

import duckdb

from .. import SCHEMA_BASE, SMALL_DATA_DIR, all_tables, timeit
from . import base

SCHEMA_DIR = SCHEMA_BASE.joinpath("schema/duckdb")


logger = logging.getLogger(__name__)


class DuckLDB(base.Connection):
    """Class for DBAPI connections to DuckLDB database"""

    def __init__(self, **kwargs):
        db_file = Duckdb_TPCH.schema_dir.joinpath("tpch.duckdb")
        # if db_file.exists():
        #     db_file.unlink()
        super().__init__(None, None, None, None, None)
        self.kwargs = kwargs
        self.db_file = db_file
        self._connection = duckdb.connect(db_file)

    def open(self):
        """Overload base connection open() with MySQL driver."""
        if self._connection is None:
            self._connection = duckdb.connect(self.db_file)
        if self._cursor is None:
            self._cursor = self._connection.cursor()
        return self._connection


class Duckdb_TPCH(base.TPCH_Runner):
    db_type = "duckdb"
    schema_dir = SCHEMA_BASE.joinpath("duckdb")

    def __init__(self, connection: DuckLDB, db_id: int, scale: str = "small"):
        super().__init__(connection, db_id, scale),
        self._conn = connection
        self.db_id = db_id

    @timeit
    def create_tables(self):
        """Create TPC-H tables.

        Note: this method requires an active DB connection.
        """
        with self._conn as conn:
            conn.query_from_file(f"{self.schema_dir}/table_schema.sql")
            conn.commit()
            print("TPC-H tables are created.")

    @timeit
    def load_single_table(
        self,
        table: str,
        delimiter: str = ",",
        data_folder: str = str(SMALL_DATA_DIR),
    ):
        """Load test data into TPC-H tables."""
        data_file = Path(data_folder).joinpath(
            self._get_datafile(Path(data_folder), table)
        )
        load_command = (
            "copy {} from '{}' with (delimiter '{}', auto_detect=false)".format(
                table, data_file, delimiter
            )
        )
        try:
            with self._conn as conn:
                conn.query(load_command)
            print(table)
        except Exception as e:
            print(f"Load data fails, exception: {e}", file=sys.stderr)

    @timeit
    def load_data(self, data_folder: str = str(SMALL_DATA_DIR), delimiter=","):
        with self._conn as conn:
            for table in all_tables:
                print("table:", table)
                data_file = Path(data_folder).joinpath(
                    self._get_datafile(Path(data_folder), table)
                )
                conn.query(
                    f"copy {table} from '{str(data_file)}' delimiter '{delimiter}'"
                )
