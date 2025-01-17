import sys
from pathlib import Path
from typing import Optional

import pymysql

from .. import DATA_DIR, timeit
from . import base

SHEMA_DIR = Path(__file__).parents[1].joinpath("schema/mysql")


class MySQLDB(base.Connection):
    """Class for DBAPI connections to MySobjectQL database"""

    def __init__(self, host, port, db_name, user, password, **kwargs):
        super().__init__(host, port, db_name, user, password)
        self.kwargs = kwargs

    def open(self):
        """Overload base connection open() with PG driver."""
        if self.__connection__ is None:
            self.__connection__ = pymysql.connect(
                host=self.host,
                port=self.port,
                database=self.db_name,
                user=self.user,
                password=self.password,
                local_infile=True,
                **self.kwargs,
            )
            self.__cursor__ = self.__connection__.cursor()
        return self.__connection__

    def _index_exists(self) -> Optional[bool]:
        """Return True if there are any IDX indexes exist, return None if no
        database connection.
        """
        if self.__cursor__ is None:
            print("database has been closed")
            return None
        idx_indexes = """
            SELECT distinct(INDEX_NAME)
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = 'tpch' AND INDEX_NAME LIKE 'IDX%';
        """
        rowcount = self.__cursor__.execute(idx_indexes)
        print("existing IDX indexes: ", rowcount)
        return rowcount == 0


class MySQL_TPCH(base.TPCH_Runner):
    db_type = "mysql"

    def __init__(self, connection: MySQLDB, scale: str = "small"):
        super().__init__(connection)
        self._conn = connection

    @timeit
    def create_tables(self):
        """Create TPC-H tables.

        Note: this method requires an active DB connection.
        """
        with self._conn as conn:
            conn.query_from_file(f"{SHEMA_DIR}/table_schema.sql")
            conn.query_from_file(f"{SHEMA_DIR}/mysql_pkeys.sql")
            conn.commit()
            print("TPC-H tables are created.")

    @timeit
    def load_single_table(self, table: str, line_terminator: Optional[str] = None):
        """Load test data into TPC-H tables."""
        data_file = f"{DATA_DIR}/{table}.csv"
        delimiter = ","
        load_command = f"""
            load data local infile '{data_file}' into table {table}
            fields terminated by '{delimiter}'
        """
        if line_terminator:
            load_command = load_command + f" lines terminated by '{line_terminator}'"
        try:
            with self._conn as conn:
                rowcount = conn.query(load_command)
                conn.commit()
            print(f"{rowcount} rows")
        except Exception as e:
            print(f"Load data fails, exception: {e}", file=sys.stderr)

    @timeit
    def after_load(self, bypass: bool = False):
        """Create indexes and analyze, optimize tables after data loading.

        Parameters:
            bypass: skip create idx indexes and continue operations even if
            found IDX indexes exist.
        """
        with self._conn as conn:
            idx_check_result = conn._index_exists()
            if idx_check_result is False:
                print("There are IDX indexes exist, remove them first.", file=sys.stderr)
                # os._exit(1)
                if not bypass:
                    return
                print(
                    "You choose to bypass, indexes create will be skipped.",
                    file=sys.stderr,
                )
            elif idx_check_result is None:
                print(
                    "IDX index check has no result, I will continue but operation may "
                    "fail if IDX index exists."
                )

            if idx_check_result is True:
                print("Create indexes")
                conn.query_from_file(f"{SHEMA_DIR}/mysql_index.sql")
                conn.commit()

            print("Analyze database")
            conn.query_from_file(f"{SHEMA_DIR}/after-load.sql")

    @timeit
    def drop_indexes(self):
        """Drop all IDX (non-primary key) indexes."""
        drop_commands = """
            SELECT distinct(CONCAT('DROP INDEX ', INDEX_NAME, ' ON ', TABLE_NAME, ';'))
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = 'tpch'
              AND INDEX_NAME LIKE 'IDX%';
        """
        try:
            print("Check if there is any indexes exist.")
            with self._conn as conn:
                index_count = conn.query(drop_commands)
                if index_count > 0:
                    for cmd in conn.fetch():
                        print()
                        # print(cmd[0])
                        conn.query(cmd[0])
                conn.commit()
        except Exception as e:
            print(f"Drop index fails, exception: {e}.", file=sys.stderr)
            return
        print("All IDX indexes are dropped.")
