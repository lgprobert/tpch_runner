"""Module for RapidsDB database TPC-H benchmark runner."""

import sys
from pathlib import Path
from typing import Iterable, Optional, Union

from pyRDP import pyrdp

from .. import DATA_DIR, SCHEMA_BASE, timeit
from . import base
from .parser import add_schema_to_table_names


class RapidsDB(base.Connection):
    """Class for DBAPI connections to RapidsDB database"""

    def __init__(self, host, port, db_name, user, password, **kwargs):
        super().__init__(host, port, db_name, user, password)
        self.kwargs = kwargs

    def open(self) -> pyrdp.Connection:
        """Overload base connection open() with RapidsDB driver."""
        if self.__connection__ is None:
            self.__connection__ = pyrdp.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.db_name,
            )
        self.__cursor__: pyrdp.Cursor = self.__connection__.cursor()  # type: ignore
        return self.__connection__

    def _ensure_impex_connector(
        self, data_path: Union[str, Path], delimiter: str = "|"
    ) -> bool:
        """Create a IMPEX connector for data loading.

        Arguments:
        - delimiter (str): column delimiter.
        - data_path (Union[str, Path]): absolute path to the data directory.
        """
        if self.__cursor__.has_connector("csv"):
            return True
        if isinstance(data_path, str):
            data_path = Path(data_path)
        if not data_path.is_dir():
            raise FileNotFoundError(f"Data directory {data_path} not found.")

        cmd = f"""
            CREATE CONNECTOR CSV TYPE IMPEX WITH PATH='{str(data_path)}',
            DELIMITER='{delimiter}'
        """
        self.__cursor__.execute(cmd)
        return True

    def query_from_file(self, filepath) -> tuple[int, Optional[Iterable], Optional[list]]:
        """Return number of rows affected by last query or -1 if database is
        closed or executing DDL statements.
        """
        if self.__cursor__ is None:
            print("database has been closed")
            return -1, None, None

        rowcount = 0
        rset = None
        columns = None

        if filepath:
            sql_script = self.read_sql(filepath)

        statements = add_schema_to_table_names(sql_script, self.db_name)

        statements = statements.split(";")
        statements = [stmt.strip() for stmt in statements if stmt.strip()]

        # breakpoint()

        try:
            for stmt in statements:
                if stmt.lower().startswith("select"):
                    self.__cursor__.execute(stmt)
                    rowcount = self.__cursor__.rowcount
                    rset = self.__cursor__.fetchall()
                    columns = [desc[0] for desc in self.__cursor__.description]
                elif stmt.startswith("--"):
                    pass
                else:
                    rowcount = self.__cursor__.execute(stmt)
                    if rowcount > 0:
                        rset = self.__cursor__.fetchall()
                        columns = [desc[0] for desc in self.__cursor__.description]

        except Exception as e:
            raise RuntimeError("Statement {} fails, exception: {}".format(stmt, e))
        return rowcount, rset, columns


class RDP_TPCH(base.TPCH_Runner):
    db_type = "rapidsdb"
    schema_dir = SCHEMA_BASE.joinpath("rapidsdb")

    def __init__(self, connection: RapidsDB, db_id: int, scale: str = "small"):
        super().__init__(connection, db_id, scale),
        self._conn: RapidsDB = connection
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
    def load_data(
        self,
        delimiter: str = ",",
        data_folder: str = str(DATA_DIR),
    ):
        """Load test data into TPC-H tables."""
        dpath = Path(data_folder)
        delimiter = delimiter
        # data_files = list(dpath.glob(f"{table}.*"))

        try:
            with self._conn as conn:
                conn._ensure_impex_connector(dpath, delimiter)
                conn.query_from_file(f"{self.schema_dir}/load.sql")
                conn.commit()
        except Exception as e:
            print(f"Load data fails, exception: {e}", file=sys.stderr)
