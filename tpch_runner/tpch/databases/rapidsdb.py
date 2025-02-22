"""Module for RapidsDB database TPC-H benchmark runner."""

import logging
import sys
from pathlib import Path
from typing import Iterable, Optional, Union

from pyrdpdb import pyrdp  # type: ignore

from .. import SCHEMA_BASE, SMALL_DATA_DIR, timeit
from . import base
from .parser import add_schema_to_table_names

logger = logging.getLogger(__name__)


class RapidsDB(base.Connection):
    """Class for DBAPI connections to RapidsDB database"""

    def __init__(self, host, port, db_name, user, password, **kwargs):
        super().__init__(host, port, db_name, user, password)
        self.kwargs = kwargs

    def open(self) -> pyrdp.Connection:
        """Overload base connection open() with RapidsDB driver."""
        if self._connection is None:
            self._connection = pyrdp.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                db=self.db_name,
            )
        self._cursor: pyrdp.Cursor = self._connection.cursor()  # type: ignore
        return self._connection

    @staticmethod
    def _parse_impex_path(connector_ddl: str) -> Optional[Path]:
        # input: "CREATE CONNECTOR CSV TYPE IMPEX WITH PATH='/home/robert/data/tpch/sf1', DELIMITER='|' NODE *" # noqa
        # result: ['PATH=', '/home/robert/data/tpch/sf1', ',']
        impex_path = [p for p in connector_ddl.split() if p.startswith("PATH")][0]

        _path = impex_path.split("'")[1]
        if _path and Path(_path).is_dir():
            return Path(_path)
        return None

    def _ensure_impex_connector(
        self, data_path: Union[str, Path], delimiter: str = "|"
    ) -> bool:
        """Create a IMPEX connector for data loading.

        Arguments:
        - delimiter (str): column delimiter.
        - data_path (Union[str, Path]): absolute path to the data directory.
        """
        replace_impex = False

        if self._cursor.has_connector("csv"):
            self.query(
                "select CONNECTOR_DDL from connectors where connector_name = 'CSV'"
            )
            _rset = self.fetch()
            connector_ddl = _rset[0][0]  # type: ignore
            if self._parse_impex_path(connector_ddl) == Path(data_path).expanduser():
                return True
            replace_impex = True

        if isinstance(data_path, str):
            data_path = Path(data_path).expanduser()
        if not data_path.is_dir():
            raise FileNotFoundError(f"Data directory {data_path} not found.")

        cmd = f"""
            CREATE CONNECTOR CSV TYPE IMPEX WITH PATH='{str(data_path)}',
            DELIMITER='{delimiter}'
        """
        if replace_impex:
            cmd = f"drop connector csv; {cmd}"
            logger.info("Recreate IMPEX connector CSV")
        self._cursor.execute(cmd)
        logger.info("IMPEX connector CSV is created.")
        return True

    def query_from_file(
        self, filepath, file_suffix: Optional[str] = None
    ) -> tuple[int, Optional[Iterable], Optional[list]]:
        """Return number of rows affected by last query or -1 if database is
        closed or executing DDL statements.
        """
        if self._cursor is None:
            self.open()
        if self._cursor is None:
            logger.error("database has been closed")
            return -1, None, None

        rowcount = 0
        rset = None
        columns = None

        if filepath:
            sql_script = self.read_sql(filepath)

        if Path(filepath).name == "load.sql" and file_suffix:
            sql_script = sql_script.replace(".tbl", file_suffix)

        statements = add_schema_to_table_names(sql_script, self.db_name)

        statements = statements.split(";")
        statements = [stmt.strip() for stmt in statements if stmt.strip()]

        try:
            for stmt in statements:
                if stmt.lower().startswith("select"):
                    self._cursor.execute(stmt)
                    rowcount = self._cursor.rowcount
                    rset = self._cursor.fetchall()
                    columns = [desc[0] for desc in self._cursor.description]
                elif stmt.startswith("--"):
                    pass
                else:
                    rowcount = self._cursor.execute(stmt)
                    if rowcount > 0:
                        rset = self._cursor.fetchall()
                        columns = [desc[0] for desc in self._cursor.description]

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

    def load_single_table(
        self,
        table: str,
        line_terminator: Optional[str] = None,
        data_folder: str = str(SMALL_DATA_DIR),
        delimiter: str = ",",
    ):
        raise NotImplementedError("RapidsDB does not support single table loading.")

    @timeit
    def load_data(
        self,
        delimiter: str = ",",
        data_folder: str = str(SMALL_DATA_DIR),
    ):
        """Load test data into TPC-H tables."""
        dpath = Path(data_folder)
        delimiter = delimiter
        # data_files = list(dpath.glob(f"{table}.*"))

        try:
            with self._conn as conn:
                conn._ensure_impex_connector(dpath, delimiter)
                data_files = dpath.glob("*")
                file_suffix = None
                if len([f for f in data_files if f.suffix == ".tbl"]) == 0:
                    any_file = next(dpath.glob("*"))
                    file_suffix = any_file.suffix

                conn.query_from_file(
                    f"{self.schema_dir}/load.sql", file_suffix=file_suffix
                )
                conn.commit()
            logger.info("TPC-H all tables are loaded.")
        except Exception as e:
            print(f"Load data fails, exception: {e}", file=sys.stderr)
