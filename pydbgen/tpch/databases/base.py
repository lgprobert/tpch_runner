from pathlib import Path
from typing import Optional

from ..gen_tool import data_gen_batch, table_map

SHEMA_DIR = "../schema/pg"
DATA_DIR = Path("~/data/tpch/small").expanduser()

all_tables = [
    "region",
    "nation",
    "customer",
    "part",
    "supplier",
    "partsupp",
    "orders",
    "lineitem",
]


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
        print("database has been closed")
        return None

    def query_from_file(self, filepath) -> int:
        """Return number of rows affected by query or -1 if database is closed
        or executing DDL statements.
        """
        if self.__cursor__ is not None:
            with open(filepath) as query_file:
                query = query_file.read()
                self.__cursor__.execute(query)
            return self.__cursor__.rowcount
        print("database has been closed")
        return -1

    def copyFrom(self, filepath, separator, table) -> int:
        """Return number of rows successfully copied into the target table."""
        if self.__cursor__ is not None:
            with open(filepath, "r") as in_file:
                self.__cursor__.copy_from(in_file, table=table, sep=separator)
            return self.__cursor__.rowcount
        print("database has been closed")
        return -1

    def commit(self) -> bool:
        if self.__connection__ is not None:
            self.__connection__.commit()
            return True
        print("cursor not initialized")
        return False


class DataGen:
    def __init__(self, sf: int = 1, delimiter: str = "|"):
        self.delimiter = delimiter
        self.scale_factor = sf

    def generate(self, table: str):
        table_code = table_map.get(table)
        data_gen_batch(table_code, self.scale_factor)  # type: ignore

    def gen_all(self):
        for tbl in table_map.keys():
            self.generate(tbl)
