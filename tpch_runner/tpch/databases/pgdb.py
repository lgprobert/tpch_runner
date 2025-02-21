"""Module for PostgreSQL database TPC-H benchmark runner."""

import logging
import sys
from pathlib import Path

import psycopg2

from .. import SCHEMA_BASE, SMALL_DATA_DIR, all_tables, timeit
from . import base

logger = logging.getLogger(__name__)


class PGDB(base.Connection):
    """Class for DBAPI connections to PostgreSQL database"""

    def __init__(self, host, port, db_name, user, password, **kwargs):
        super().__init__(host, port, db_name, user, password)
        self.kwargs = kwargs

    def open(self):
        """Overload base connection open() with PG driver."""
        if self._connection is None:
            self._connection = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                **self.kwargs,
            )
            self._cursor = self._connection.cursor()
        return self._connection

    def copyFrom(self, filepath, separator, table) -> int:
        """Return number of rows successfully copied into the target table."""
        if self._cursor is None:
            self.open()
        if self._cursor is None:
            logger.error("database has been closed")
            return -1

        logger.info(f"Load table {table} from {filepath}.")
        with open(filepath, "r") as in_file:
            self._cursor.copy_expert(
                f"COPY {table} FROM STDIN WITH (format CSV, delimiter ',',  QUOTE '\"')",  # noqa
                in_file,
            )
        return self._cursor.rowcount


class PG_TPCH(base.TPCH_Runner):
    db_type = "pg"
    schema_dir = SCHEMA_BASE.joinpath("pg")

    def __init__(self, connection: PGDB, db_id: int, scale: str = "small"):
        super().__init__(connection, db_id, scale),
        self._conn = connection
        self.db_id = db_id

    @timeit
    def create_tables(self):
        """Create TPC-H tables.

        Note: this method requires an active DB connection.
        """
        with self._conn as conn:
            print("Create tables")
            conn.query_from_file(f"{self.schema_dir}/table_schema.sql")
            print("Add primary keys")
            conn.query_from_file(f"{self.schema_dir}/pg_constraints.sql")
            conn.commit()
        print("TPC-H tables are created.")

    @timeit
    def load_single_table(
        self,
        table: str,
        data_folder: str = str(SMALL_DATA_DIR),
        delimiter: str = ",",
    ):
        """Load test data into TPC-H tables."""
        data_file = Path(data_folder).joinpath(
            self._get_datafile(Path(data_folder), table)
        )
        try:
            with self._conn as conn:
                conn.copyFrom(data_file, delimiter, table)
                conn.commit()
        except Exception as e:
            print(f"Load data fails, exception: {e}", file=sys.stderr)

    @timeit
    def load_data(
        self,
        table: str = "all",
        data_folder: str = str(SMALL_DATA_DIR),
        delimiter: str = ",",
    ):
        if table != "all" and table not in all_tables:
            raise ValueError(f"Invalid table name {table}.")
        elif table != "all":
            self.load_single_table(table, data_folder=data_folder, delimiter=delimiter)
        else:
            for tbl in all_tables:
                self.load_single_table(tbl, data_folder=data_folder, delimiter=delimiter)
            print("All tables finish loading.")

    @timeit
    def after_load(self, reindex: bool = False):
        with self._conn as conn:
            if reindex:
                print("Create indexes.")
                conn.query_from_file(f"{self.schema_dir}/pg_index.sql")
                conn.commit()

            print("\nAnalyze database")
            conn.query("analyze")
