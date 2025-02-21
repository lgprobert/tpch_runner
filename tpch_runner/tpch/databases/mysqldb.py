"""Module for MySQL database TPC-H benchmark runner."""

import logging
import sys
from pathlib import Path
from typing import Optional

import pymysql

from .. import SCHEMA_BASE, SMALL_DATA_DIR, timeit
from . import base

logger = logging.getLogger(__name__)


class MySQLDB(base.Connection):
    """Class for DBAPI connections to MySQL database"""

    def __init__(self, host, port, db_name, user, password, **kwargs):
        super().__init__(host, port, db_name, user, password)
        self.kwargs = kwargs

    def open(self):
        """Overload base connection open() with MySQL driver."""
        if self._connection is None:
            self._connection = pymysql.connect(
                host=self.host,
                port=self.port,
                database=self.db_name,
                user=self.user,
                password=self.password,
                local_infile=True,
                **self.kwargs,
            )
            self._cursor = self._connection.cursor()
        return self._connection

    def _index_exists(self) -> Optional[bool]:
        """Return True if there are any IDX indexes exist, return None if no
        database connection.
        """
        if self._cursor is None:
            self.open()
        if self._cursor is None:
            logger.error("database has been closed")
            return None
        idx_indexes = """
            SELECT distinct(INDEX_NAME)
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = 'tpch' AND INDEX_NAME LIKE 'IDX%';
        """
        rowcount = self._cursor.execute(idx_indexes)
        logger.debug(f"existing IDX indexes: {rowcount}.\n")
        return rowcount > 0


class MySQL_TPCH(base.TPCH_Runner):
    db_type = "mysql"
    schema_dir = SCHEMA_BASE.joinpath("mysql")

    def __init__(self, connection: MySQLDB, db_id: int, scale: str = "small"):
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
            conn.query_from_file(f"{self.schema_dir}/mysql_pkeys.sql")
            conn.commit()
            print("TPC-H tables are created.")

    @timeit
    def load_single_table(
        self,
        table: str,
        line_terminator: Optional[str] = None,
        delimiter: str = ",",
        data_folder: str = str(SMALL_DATA_DIR),
    ):
        """Load test data into TPC-H tables."""
        data_file = Path(data_folder).joinpath(
            self._get_datafile(Path(data_folder), table)
        )
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
            print(f"{table}: {rowcount} rows")
        except Exception as e:
            print(f"Load data fails, exception: {e}", file=sys.stderr)

    def before_load(self, reindex: bool = False):
        with self._conn as conn:
            try:
                conn.query("set autocommit = 0;")
                conn.query("set unique_checks = 0;")
                conn.query("set foreign_key_checks = 0;")
                conn.query("set sql_log_bin = 0;")
                if reindex:
                    self.drop_indexes()
            except Exception as e:
                logger.error("You don't have permission to run priliged commands.")
                logger.error(f"Exception: {e}")
                return

    @timeit
    def after_load(self, reindex: bool = False):
        """Create indexes and analyze, optimize tables after data loading.

        Parameters:
            bypass: skip create idx indexes and continue operations even if
            found IDX indexes exist.
        """
        with self._conn as conn:
            try:
                conn.query("set autocommit = 1;")
                conn.query("set unique_checks = 1;")
                conn.query("set foreign_key_checks = 1;")
                conn.query("set sql_log_bin = 1;")
            except Exception as e:
                logger.error("You don't have permission to run priliged commands.")
                logger.error(f"Exception: {e}")
                return

            if reindex:
                idx_check_result = conn._index_exists()
                if idx_check_result is False:
                    print(
                        "There are IDX indexes exist, remove them first.", file=sys.stderr
                    )
                    self.drop_indexes()
                logger.info("Create indexes")
                conn.query_from_file(f"{self.schema_dir}/mysql_index.sql")
                conn.commit()

            logger.info("Analyze database")
            conn.query_from_file(f"{self.schema_dir}/after-load.sql")

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
            print("\nCheck if there is any indexes exist.")
            with self._conn as conn:
                index_count = conn.query(drop_commands)
                if index_count > 0:
                    for cmd in conn.fetch():
                        conn.query(cmd[0])
                conn.commit()
        except Exception as e:
            print(f"Drop index fails, exception: {e}.", file=sys.stderr)
            return
        print("All IDX indexes are dropped.\n")
