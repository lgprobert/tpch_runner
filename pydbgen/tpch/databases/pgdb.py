import sys

import psycopg2

from .base import DATA_DIR, Connection, all_tables  # type: ignore

SHEMA_DIR = "../schema/pg"
QUERY_DIR = "../queries"


class PGDB(Connection):
    """Class for DBAPI connections to PostgreSQL database"""

    def __init__(self, host, port, db_name, user, password):
        super().__init__(host, port, db_name, user, password)

    def open(self):
        """Overload base connection open() with PG driver."""
        if self.__connection__ is None:
            self.__connection__ = psycopg2.connect(
                host=self.host,
                port=self.port,
                dbname=self.db_name,
                user=self.user,
                password=self.password,
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


class PG_TPCH:
    def __init__(self, host, port, db_name, user, password):
        self.conn = PGDB(host, port, db_name, user, password)

    def create_tables(self):
        """Create TPC-H tables.

        Note: this method requires an active DB connection.
        """
        with self.conn as conn:
            print("Create tables")
            conn.query_from_file(f"{SHEMA_DIR}/table_schema.sql")
            print("Add primary keys")
            conn.query_from_file(f"{SHEMA_DIR}/pg_constraints.sql")
            conn.commit()

    def load_single_table(self, table: str):
        """Load test data into TPC-H tables."""
        try:
            with self.conn as conn:
                conn.copyFrom(f"{DATA_DIR}/{table}.csv", ",", table)
                conn.commit()
        except Exception as e:
            print(f"Load data fails, exception: {e}", file=sys.stderr)

    def load_data(self, table: str = "all"):
        if table != "all" and table not in all_tables:
            raise ValueError(f"Invalid table name {table}.")
        elif table != "all":
            self.load_single_table(table)
        else:
            for tbl in all_tables:
                self.load_single_table(tbl)
            print("All tables finish loading.")

    def after_load(self):
        with self.conn as conn:
            print("Create indexes")
            conn.query_from_file(f"{SHEMA_DIR}/pg_index.sql")
            conn.commit()

            print("Analyze database")
            conn.query("analyze")

    def run_query(self, query_index: int):
        try:
            with self.conn as conn:
                rowcount = conn.query_from_file(f"{QUERY_DIR}/{query_index}.sql")
                # print(self.conn.fetch())
                print(f"Q{query_index} succeeds, return {rowcount} rows.")
                return self.conn.fetch()
        except Exception as e:
            print(f"Query execution fails, exception: {e}", file=sys.stderr)
