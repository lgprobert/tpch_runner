import sys
from pathlib import Path
from typing import Iterable, Optional

import pymysql

from .. import DATA_DIR, all_tables, timeit
from . import QUERY_DIR, base

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
                **self.kwargs,
            )
            self.__cursor__ = self.__connection__.cursor()
        return self.__connection__

    def query_from_file(self, filepath) -> tuple[int, Optional[Iterable]]:
        """Return number of rows affected by last query or -1 if database is
        closed or executing DDL statements.
        """
        if self.__cursor__ is None:
            print("database has been closed")
            return -1, None
        with open(filepath) as query_file:
            sql_script = query_file.read()
            statements = sql_script.split(";")

            for _statement in statements:
                try:
                    _statement = _statement.strip()
                    if _statement:
                        if (
                            "create view" in _statement.lower()
                            or "drop view" in _statement.lower()
                        ):
                            self.__cursor__.execute(_statement)
                        else:
                            rowcount = self.__cursor__.execute(_statement)
                            rset = self.__cursor__.fetchall()
                except Exception as e:
                    raise RuntimeError(
                        "Statement {} fails, exception: {}".format(_statement, e)
                    )
        return rowcount, rset

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


class MySQL_TPCH:
    def __init__(self, host, port, db_name, user, password, **kwargs):
        self.conn = MySQLDB(host, port, db_name, user, password, **kwargs)

    @timeit
    def create_tables(self):
        """Create TPC-H tables.

        Note: this method requires an active DB connection.
        """
        with self.conn as conn:
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
            with self.conn as conn:
                rowcount = conn.query(load_command)
                conn.commit()
            print(f"{rowcount} rows")
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
                print()
                self.load_single_table(tbl)
            print("\nAll tables finish loading.")

    @timeit
    def after_load(self, bypass: bool = False):
        """Create indexes and analyze, optimize tables after data loading.

        Parameters:
            bypass: skip create idx indexes and continue operations even if
            found IDX indexes exist.
        """
        with self.conn as conn:
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
    def run_query(self, query_index: int) -> tuple[int, Optional[Iterable]]:
        try:
            with self.conn as conn:
                rowcount, rset = conn.query_from_file(f"{QUERY_DIR}/{query_index}.sql")
                # print(self.conn.fetch())
                print(f"\nQ{query_index} succeeds, return {rowcount} rows.")
                # rset = self.conn.fetch()
        except Exception as e:
            print(f"Query execution fails, exception: {e}", file=sys.stderr)
        return rowcount, rset

    @timeit
    def power_test(self):
        """Run TPC-H power test."""
        results = {}
        # breakpoint()
        for _query_idx in base.QUERY_ORDER[0]:
            rowcount, rset, runtime = self.run_query(_query_idx)
            results[_query_idx] = {"rows": rowcount, "result": rset, "time": runtime}
        print("\nPowertest is finished.")
        return results

    @timeit
    def drop_all(self):
        try:
            with self.conn as conn:
                conn.drop_all()
        except Exception as e:
            print(f"Drop table fails, exception: {e}.", file=sys.stderr)
            return
        print("All tables are dropped.")

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
            with self.conn as conn:
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
