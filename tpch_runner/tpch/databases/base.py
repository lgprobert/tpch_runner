import abc
import logging
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

import sqlglot

from ...meta import TestResultManager, setup_database
from .. import (
    QUERY_ORDER,
    RESULT_DIR,
    SCHEMA_BASE,
    SMALL_DATA_DIR,
    InternalQueryArgs,
    Result,
    all_tables,
    post_process,
    timeit,
)

POWER = "power"
THROUGHPUT = "throughput"
QUERY_METRIC = "query_stream_%s_query_%s"
REFRESH_METRIC = "refresh_stream_%s_func_%s"
THROUGHPUT_TOTAL_METRIC = "throughput_test_total"
NUM_QUERIES = len(QUERY_ORDER[0])

logger = logging.getLogger(__name__)


class Connection(abc.ABC):
    """Class for DBAPI connections to PostgreSQL database"""

    _connection = None
    _cursor = None

    def __init__(self, host, port, db_name, user, password, **kwargs):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self._connection = None
        self._cursor = None

    @abc.abstractmethod
    def open(self):
        """Establish DB connection and create a cursor, return connection."""
        pass

    def close(self) -> None:
        if self._cursor is not None:
            self._cursor.close()
            self._cursor = None
        if self._connection is not None:
            self._connection.close()
            self._connection = None

    def __enter__(self):
        """Context manager entry point."""
        self.open()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """Context manager exit point."""
        self.close()

    @staticmethod
    def read_sql(filepath: str) -> str:
        with open(filepath) as query_file:
            content = query_file.readlines()

        content = [
            line for line in content if line.strip() and not line.strip().startswith("--")
        ]
        _text = " ".join(content)
        _text = _text.replace("\n", " ").replace("\t", " ")
        return _text

    def normalize_query(self, query: str) -> str:
        parsed = sqlglot.parse_one(query)

        # Function to prepend schema name to table names
        def prepend_schema(node, schema):
            if isinstance(node, sqlglot.expressions.Table):
                # Prepend schema name to table names
                node.args["db"] = schema
            return node

        # Transform the AST
        transformed = parsed.transform(prepend_schema, schema=self.db_name)

        # Generate the transformed SQL query
        transformed_query = transformed.sql()
        return transformed_query

    def query(self, query: str) -> int:
        """Execute a query from connection cursor."""
        if self._cursor is None:
            self.open()
        if self._cursor is None:
            logger.error("database has been closed")
            return -1
        self._cursor.execute(query)
        return self._cursor.rowcount

    def fetch(self) -> Optional[list[tuple]]:
        if self._cursor is None:
            self.open()
        if self._cursor is None:
            logger.error("database has been closed")
            return None
        if self._cursor.description:
            return self._cursor.fetchall()
        return None

    def query_from_file(self, filepath) -> tuple[int, Optional[Iterable], Optional[list]]:
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

        raw_statements = sql_script.split(";")
        statements = [stmt.strip() for stmt in raw_statements if stmt.strip()]

        # statements = []
        # for stmt in raw_statements:
        #     if stmt.strip():
        #         stmt = self.normalize_query(stmt)
        #         statements.append(stmt + ";")

        try:
            for stmt in statements:
                if stmt.lower().startswith("select"):
                    self._cursor.execute(stmt)
                    rset = self._cursor.fetchall()
                    rowcount = len(rset)
                    columns = [desc[0] for desc in self._cursor.description]
                elif (
                    stmt.lower().startswith("create")
                    or stmt.lower().startswith("update")
                    or stmt.lower().startswith("drop")
                ):
                    self._cursor.execute(stmt)
                else:
                    self._cursor.execute(stmt)
                    rset = self._cursor.fetchall()
                    columns = [desc[0] for desc in self._cursor.description]
                    rowcount = len(rset)

        except Exception as e:
            raise RuntimeError("Statement {} fails, exception: {}".format(stmt, e))
        return rowcount, rset, columns

    def commit(self) -> bool:
        if self._cursor is None:
            print("cursor not initialized")
            return False
        self._connection.commit()
        return True


class TPCH_Runner:
    db_type = ""
    query_dir = Path(__file__).parents[1].joinpath("queries")
    schema_dir = SCHEMA_BASE.joinpath("schema").joinpath(db_type)

    def __init__(self, connection: Connection, db_id: int, scale: str = "small"):
        self._conn = connection
        self.meta = TestResultManager(setup_database())
        self.scale = scale
        self.db_id = db_id

    def create_tables(self):
        pass

    @staticmethod
    def _get_datafile(data_folder: Path, table_name: str):
        data_files = data_folder.glob(f"{table_name}.*")
        first_file = next(data_files)
        if first_file.suffix != ".csv":
            return table_name + first_file.suffix
        return table_name + ".csv"

    def load_single_table(
        self,
        table: str,
        line_terminator: Optional[str] = None,
        data_folder: str = str(SMALL_DATA_DIR),
        delimiter: str = ",",
    ):
        pass

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
                print("table:", table)
                self.load_single_table(tbl, data_folder=data_folder, delimiter=delimiter)
            print()
            logger.info("All tables finish loading.")

    def truncate_table(self, table: str = "all"):
        try:
            with self._conn as conn:
                if table == "all":
                    for tbl in all_tables:
                        conn.query(f"truncate table {tbl}")
                else:
                    conn.query(f"truncate table {table}")
                conn.commit()
        except Exception as e:
            print(f"Truncate table fails, exception: {e}.", file=sys.stderr)
            return
        logger.info(f"Table {table} is truncated.")

    @timeit
    def drop_table(self, table: str = "all"):
        try:
            with self._conn as conn:
                if table == "all":
                    for tbl in all_tables:
                        conn.query(f"drop table if exists {tbl}")
                else:
                    conn.query(f"drop table if exists {table}")
                conn.commit()
        except Exception as e:
            print(f"Drop table fails, exception: {e}.", file=sys.stderr)
            return
        logger.info(f"Table {table} are dropped.")

    @post_process
    @timeit
    def run_query(
        self, query_index: int, result_dir: Optional[Path] = None, no_report: bool = False
    ) -> tuple[Result, float, Any]:
        """Run a TPC-H query from query file.

        Return:
            Result(
                rowcount (int): number of records.
                rset (tuple): resultset.
                columns (list): column names in resultset.
                result_file (str): result file path.
            )
            runtime (float): query runtime.
            internal_args (InternalQueryArgs | None): internal arguments.
        """
        _internal_args = InternalQueryArgs(
            db=self.db_type,
            idx=query_index,
            result_dir=result_dir,
            no_report=no_report,
            metadb=self.meta,
            db_id=self.db_id,
        )
        try:
            with self._conn as conn:
                custom_query_folder = self.schema_dir.joinpath("queries")
                if not custom_query_folder.joinpath(f"q{query_index}.sql").exists():
                    query_file = f"{self.query_dir}/q{query_index}.sql"
                else:
                    query_file = f"{custom_query_folder}/q{query_index}.sql"
                rowcount, rset, columns = conn.query_from_file(query_file)
                print(f"\nQ{query_index} succeeds, return {rowcount} rows.")
            result = Result(
                success=True,
                rowcount=rowcount,
                rset=rset,
                columns=columns,
                result_file=None,
            )
            return result, 0, _internal_args
        except Exception as e:
            print(f"Query execution fails, exception: {e}", file=sys.stderr)
        return Result(False, -1, None, None, None), 0, _internal_args

    def power_test(self, no_report: bool = False):
        """Run TPC-H power test."""
        results = {}
        total_time = 0
        success = True

        test_time, result_folder = self.meta.add_powertest(
            db_id=self.db_id, db_type=self.db_type, scale=self.scale, no_report=no_report
        )
        result_dir = RESULT_DIR.joinpath(result_folder)
        logger.info(f"Test result will be saved in: {result_dir}")
        result_dir.mkdir(exist_ok=True)
        print()
        logger.info(f"Power test start at {test_time.strftime('%Y-%m-%d %H:%M:%S')}")

        result: Result
        for _query_idx in QUERY_ORDER[0]:
            result, runtime, _ = self.run_query(_query_idx, result_dir, no_report)
            (query_success, rowcount, rset, _, _) = result
            results[_query_idx] = {"rows": rowcount, "result": rset, "time": runtime}
            total_time += runtime
            if query_success is False:
                success = False

        if not no_report:
            self.meta.update_powertest(
                result_folder=str(result_dir.stem), success=success, runtime=total_time
            )

        print()
        logger.info(
            "Powertest is finished, test result: {}, total time: {} secs.".format(
                "Succeed" if success else "Fail", round(total_time, 6)
            )
        )
        return results

    def after_load(self, reindex: bool = False):
        pass

    def before_load(self, reindex: bool = False):
        pass

    def count_rows(self, table_name: str):
        """Return number of rows in the given table or all tables."""
        table_info = {}
        with self._conn as conn:
            if table_name != "all":
                conn.query(f"select count(*) from {table_name}")
                table_info[table_name] = conn.fetch()[0][0]
            else:
                for tbl in all_tables:
                    conn.query(f"select count(*) from {tbl}")
                    table_info[tbl] = conn.fetch()[0][0]
        return table_info
