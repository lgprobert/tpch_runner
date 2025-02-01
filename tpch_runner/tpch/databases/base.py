import logging
import sys
from pathlib import Path
from typing import Any, Iterable, Optional

import sqlglot

from ...meta import TestResultManager, setup_database
from .. import (
    DATA_DIR,
    QUERY_ORDER,
    RESULT_DIR,
    SCHEMA_BASE,
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


# QUERY_ORDER = [  # follows appendix A of the TPCH-specification
#     [14, 2, 9, 20, 6, 17, 18, 8, 21, 13, 3, 22, 16, 4, 11, 15, 1, 10, 19, 5, 7, 12],
#     [21, 3, 18, 5, 11, 7, 6, 20, 17, 12, 16, 15, 13, 10, 2, 8, 14, 19, 9, 22, 1, 4],
#     [6, 17, 14, 16, 19, 10, 9, 2, 15, 8, 5, 22, 12, 7, 13, 18, 1, 4, 20, 3, 11, 21],
#     [8, 5, 4, 6, 17, 7, 1, 18, 22, 14, 9, 10, 15, 11, 20, 2, 21, 19, 13, 16, 12, 3],
#     [5, 21, 14, 19, 15, 17, 12, 6, 4, 9, 8, 16, 11, 2, 10, 18, 1, 13, 7, 22, 3, 20],
#     [21, 15, 4, 6, 7, 16, 19, 18, 14, 22, 11, 13, 3, 1, 2, 5, 8, 20, 12, 17, 10, 9],
#     [10, 3, 15, 13, 6, 8, 9, 7, 4, 11, 22, 18, 12, 1, 5, 16, 2, 14, 19, 20, 17, 21],
#     [18, 8, 20, 21, 2, 4, 22, 17, 1, 11, 9, 19, 3, 13, 5, 7, 10, 16, 6, 14, 15, 12],
#     [19, 1, 15, 17, 5, 8, 9, 12, 14, 7, 4, 3, 20, 16, 6, 22, 10, 13, 2, 21, 18, 11],
#     [8, 13, 2, 20, 17, 3, 6, 21, 18, 11, 19, 10, 15, 4, 22, 1, 7, 12, 9, 14, 5, 16],
#     [6, 15, 18, 17, 12, 1, 7, 2, 22, 13, 21, 10, 14, 9, 3, 16, 20, 19, 11, 4, 8, 5],
#     [15, 14, 18, 17, 10, 20, 16, 11, 1, 8, 4, 22, 5, 12, 3, 9, 21, 2, 13, 6, 19, 7],
#     [1, 7, 16, 17, 18, 22, 12, 6, 8, 9, 11, 4, 2, 5, 20, 21, 13, 10, 19, 3, 14, 15],
#     [21, 17, 7, 3, 1, 10, 12, 22, 9, 16, 6, 11, 2, 4, 5, 14, 8, 20, 13, 18, 15, 19],
#     [2, 9, 5, 4, 18, 1, 20, 15, 16, 17, 7, 21, 13, 14, 19, 8, 22, 11, 10, 3, 12, 6],
#     [16, 9, 17, 8, 14, 11, 10, 12, 6, 21, 7, 3, 15, 5, 22, 20, 1, 13, 19, 2, 4, 18],
#     [1, 3, 6, 5, 2, 16, 14, 22, 17, 20, 4, 9, 10, 11, 15, 8, 12, 19, 18, 13, 7, 21],
#     [3, 16, 5, 11, 21, 9, 2, 15, 10, 18, 17, 7, 8, 19, 14, 13, 1, 4, 22, 20, 6, 12],
#     [14, 4, 13, 5, 21, 11, 8, 6, 3, 17, 2, 20, 1, 19, 10, 9, 12, 18, 15, 7, 22, 16],
#     [4, 12, 22, 14, 5, 15, 16, 2, 8, 10, 17, 9, 21, 7, 3, 6, 13, 18, 11, 20, 19, 1],
#     [16, 15, 14, 13, 4, 22, 18, 19, 7, 1, 12, 17, 5, 10, 20, 3, 9, 21, 11, 2, 6, 8],
#     [20, 14, 21, 12, 15, 17, 4, 19, 13, 10, 11, 1, 16, 5, 18, 7, 8, 22, 9, 6, 3, 2],
#     [16, 14, 13, 2, 21, 10, 11, 4, 1, 22, 18, 12, 19, 5, 7, 8, 6, 3, 15, 20, 9, 17],
#     [18, 15, 9, 14, 12, 2, 8, 11, 22, 21, 16, 1, 6, 17, 5, 10, 19, 4, 20, 13, 3, 7],
#     [7, 3, 10, 14, 13, 21, 18, 6, 20, 4, 9, 8, 22, 15, 2, 1, 5, 12, 19, 17, 11, 16],
#     [18, 1, 13, 7, 16, 10, 14, 2, 19, 5, 21, 11, 22, 15, 8, 17, 20, 3, 4, 12, 6, 9],
#     [13, 2, 22, 5, 11, 21, 20, 14, 7, 10, 4, 9, 19, 18, 6, 3, 1, 8, 15, 12, 17, 16],
#     [14, 17, 21, 8, 2, 9, 6, 4, 5, 13, 22, 7, 15, 3, 1, 18, 16, 11, 10, 12, 20, 19],
#     [10, 22, 1, 12, 13, 18, 21, 20, 2, 14, 16, 7, 15, 3, 4, 17, 5, 19, 6, 8, 9, 11],
#     [10, 8, 9, 18, 12, 6, 1, 5, 20, 11, 17, 22, 16, 3, 13, 2, 15, 21, 14, 19, 7, 4],
#     [7, 17, 22, 5, 3, 10, 13, 18, 9, 1, 14, 15, 21, 19, 16, 12, 8, 6, 11, 20, 4, 2],
#     [2, 9, 21, 3, 4, 7, 1, 11, 16, 5, 20, 19, 18, 8, 17, 13, 10, 12, 15, 6, 14, 22],
#     [15, 12, 8, 4, 22, 13, 16, 17, 18, 3, 7, 5, 6, 1, 9, 11, 21, 10, 14, 20, 19, 2],
#     [15, 16, 2, 11, 17, 7, 5, 14, 20, 4, 21, 3, 10, 9, 12, 8, 13, 6, 18, 19, 22, 1],
#     [1, 13, 11, 3, 4, 21, 6, 14, 15, 22, 18, 9, 7, 5, 10, 20, 12, 16, 17, 8, 19, 2],
#     [14, 17, 22, 20, 8, 16, 5, 10, 1, 13, 2, 21, 12, 9, 4, 18, 3, 7, 6, 19, 15, 11],
#     [9, 17, 7, 4, 5, 13, 21, 18, 11, 3, 22, 1, 6, 16, 20, 14, 15, 10, 8, 2, 12, 19],
#     [13, 14, 5, 22, 19, 11, 9, 6, 18, 15, 8, 10, 7, 4, 17, 16, 3, 1, 12, 2, 21, 20],
#     [20, 5, 4, 14, 11, 1, 6, 16, 8, 22, 7, 3, 2, 12, 21, 19, 17, 13, 10, 15, 18, 9],
#     [3, 7, 14, 15, 6, 5, 21, 20, 18, 10, 4, 16, 19, 1, 13, 9, 8, 17, 11, 12, 22, 2],
#     [13, 15, 17, 1, 22, 11, 3, 4, 7, 20, 14, 21, 9, 8, 2, 18, 16, 6, 10, 12, 5, 19],
# ]
NUM_QUERIES = len(QUERY_ORDER[0])

logger = logging.getLogger(__name__)


class Connection:
    """Class for DBAPI connections to PostgreSQL database"""

    __connection__ = None
    __cursor__ = None

    def __init__(self, host, port, db_name, user, password):
        self.host = host
        self.port = port
        self.db_name = db_name
        self.user = user
        self.password = password
        self.__connection__ = None
        self.__cursor__ = None

    def open(self):
        """Establish DB connection and create a cursor, return connection."""
        return self.__connection__

    def close(self) -> None:
        if self.__cursor__ is not None:
            self.__cursor__.close()
            self.__cursor__ = None
        if self.__connection__ is not None:
            self.__connection__.close()
            self.__connection__ = None

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
        if self.__cursor__:
            self.__cursor__.execute(query)
            return self.__cursor__.rowcount
        print("database has been closed")
        return -1

    def fetch(self) -> Optional[list[tuple]]:
        if self.__cursor__:
            if self.__cursor__.description:
                return self.__cursor__.fetchall()
            return None
        print("database has been closed")
        return None

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
                    self.__cursor__.execute(stmt)
                    rset = self.__cursor__.fetchall()
                    rowcount = len(rset)
                    columns = [desc[0] for desc in self.__cursor__.description]
                elif (
                    stmt.lower().startswith("create")
                    or stmt.lower().startswith("update")
                    or stmt.lower().startswith("drop")
                ):
                    self.__cursor__.execute(stmt)
                else:
                    self.__cursor__.execute(stmt)
                    rset = self.__cursor__.fetchall()
                    columns = [desc[0] for desc in self.__cursor__.description]
                    rowcount = len(rset)

        except Exception as e:
            raise RuntimeError("Statement {} fails, exception: {}".format(stmt, e))
        return rowcount, rset, columns

    def commit(self) -> bool:
        if self.__connection__ is None:
            print("cursor not initialized")
            return False
        self.__connection__.commit()
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
        data_folder: str = str(DATA_DIR),
        delimiter: str = ",",
    ):
        pass

    @timeit
    def load_data(
        self,
        table: str = "all",
        data_folder: str = str(DATA_DIR),
        delimiter: str = ",",
    ):
        if table != "all" and table not in all_tables:
            raise ValueError(f"Invalid table name {table}.")
        elif table != "all":
            self.load_single_table(table, data_folder=data_folder, delimiter=delimiter)
        else:
            for tbl in all_tables:
                print()
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
