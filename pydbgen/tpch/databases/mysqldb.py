# from typing import Optional

import pymysql

from .base import Connection  # type: ignore

SHEMA_DIR = "../schema/memsql"


class MySQLDB(Connection):
    """Class for DBAPI connections to MySQL database"""

    def __init__(self, host, port, db_name, user, password):
        super().__init__(host, port, db_name, user, password)

    def open(self):
        """Overload base connection open() with PG driver."""
        if self.__connection__ is None:
            self.__connection__ = pymysql.connect(
                host=self.host,
                port=self.port,
                database=self.db_name,
                user=self.user,
                password=self.password,
            )
            self.__cursor__ = self.__connection__.cursor()
        return self.__connection__


class MySQL_TPCH:
    def __init__(self, host, port, db_name, user, password):
        self.conn = MySQLDB(host, port, db_name, user, password)

    def create_tables(self):
        """Create TPC-H tables.

        Note: this method requires an active DB connection.
        """
        with self.conn as conn:
            conn.query_from_file(f"{SHEMA_DIR}/table_schema.sql")
            conn.commit()
