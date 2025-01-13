import sys
from pathlib import Path

import psycopg2

from .. import DATA_DIR, all_tables, timeit
from . import base

SHEMA_DIR = Path(__file__).parents[1].joinpath("schema/pg")


class PGDB(base.Connection):
    """Class for DBAPI connections to PostgreSQL database"""

    def __init__(self, host, port, db_name, user, password, **kwargs):
        super().__init__(host, port, db_name, user, password)
        self.kwargs = kwargs

    def open(self):
        """Overload base connection open() with PG driver."""
        if self.__connection__ is None:
            self.__connection__ = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.db_name,
                user=self.user,
                password=self.password,
                **self.kwargs,
            )
            self.__cursor__ = self.__connection__.cursor()
        return self.__connection__

    def copyFrom(self, filepath, separator, table) -> int:
        """Return number of rows successfully copied into the target table."""
        if self.__cursor__ is not None:
            print(f"Load table {table} from {filepath}.")
            with open(filepath, "r") as in_file:
                self.__cursor__.copy_from(in_file, table=table, sep=separator)
            return self.__cursor__.rowcount
        print("database has been closed")
        return -1


class PG_TPCH(base.TPCH_Runner):
    db_type = "pg"

    def __init__(self, connection: PGDB):
        super().__init__(connection)
        self._conn = connection

    @timeit
    def create_tables(self):
        """Create TPC-H tables.

        Note: this method requires an active DB connection.
        """
        with self._conn as conn:
            print("Create tables")
            conn.query_from_file(f"{SHEMA_DIR}/table_schema.sql")
            print("Add primary keys")
            conn.query_from_file(f"{SHEMA_DIR}/pg_constraints.sql")
            conn.commit()
        print("TPC-H tables are created.")

    @timeit
    def load_single_table(self, table: str):
        """Load test data into TPC-H tables."""
        try:
            with self._conn as conn:
                conn.copyFrom(f"{DATA_DIR}/{table}.csv", ",", table)
                conn.commit()
        except Exception as e:
            print(f"Load data fails, exception: {e}", file=sys.stderr)

    @timeit
    def load_data(self, table: str = "all"):
        if table != "all" and table not in all_tables:
            raise ValueError(f"Invalid table name {table}.")
        elif table != "all":
            self.load_single_table(table)
        else:
            for tbl in all_tables:
                self.load_single_table(tbl)
            print("All tables finish loading.")

    @timeit
    def after_load(self):
        with self._conn as conn:
            print("Create indexes")
            conn.query_from_file(f"{SHEMA_DIR}/pg_index.sql")
            conn.commit()

            print("Analyze database")
            conn.query("analyze")
