import logging
import shutil
import time
from datetime import datetime
from importlib import import_module
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Engine,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
    event,
)
from sqlalchemy.exc import DatabaseError
from sqlalchemy.orm import DeclarativeBase, Mapped, joinedload, relationship, sessionmaker

from tpch_runner.config import Config

from .tpch import RESULT_DIR
from .tpch.databases.results import Result

logger = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


db_classes = {
    "mysql": ".tpch.databases.mysqldb.MySQLDB",
    "pg": ".tpch.databases.pgdb.PGDB",
    "rapidsdb": ".tpch.databases.rapidsdb.RapidsDB",
    "duckdb": ".tpch.databases.duckdb.DuckLDB",
}


class Database(Base):  # type: ignore
    __tablename__ = "databases"
    __table_args__ = {"sqlite_autoincrement": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    db_type = Column(String, nullable=False)
    alias = Column(String, nullable=True, unique=True)
    host = Column(String, nullable=False)
    port = Column(String, nullable=False)
    user = Column(String, nullable=False)
    password = Column(String, nullable=False)
    dbname = Column(String, nullable=False)
    version = Column(String, nullable=True)
    config = Column(String, nullable=True)
    scale = Column(String, nullable=False, default="small")
    description = Column(String, nullable=True)


class TestResult(Base):  # type: ignore
    __tablename__ = "results"
    __table_args__ = {"sqlite_autoincrement": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    testtime = Column(DateTime, default=datetime.utcnow, nullable=False)
    db_type = Column(String, nullable=False)
    success = Column(Boolean, nullable=False)

    rowcount = Column(Integer, nullable=False)
    result_csv = Column(String, nullable=False)
    query_name = Column(String, nullable=False)
    runtime = Column(Float, nullable=False, default=0)
    result_folder = Column(
        String, ForeignKey("powertests.result_folder", ondelete="CASCADE")
    )
    database_id = Column(Integer, ForeignKey("databases.id"), nullable=False)
    database = relationship("Database", backref="results")
    power_test: Mapped["PowerTest"] = relationship(back_populates="results")


class PowerTest(Base):  # type: ignore
    __tablename__ = "powertests"
    __table_args__ = {"sqlite_autoincrement": True}

    id = Column(Integer, primary_key=True, autoincrement=True)
    db_type = Column(String, nullable=False)
    scale = Column(String, nullable=False)
    result_folder = Column(String, unique=True, nullable=False)
    testtime = Column(DateTime, default=datetime.utcnow, nullable=False)
    success = Column(Boolean, nullable=True)
    runtime = Column(Float, default=0, nullable=True)
    comment = Column(String, nullable=True)
    database_id = Column(Integer, ForeignKey("databases.id"), nullable=False)
    database = relationship("Database", backref="powertests")
    results: Mapped[list["TestResult"]] = relationship(  # Explicit annotation
        back_populates="power_test", passive_deletes=True
    )


def setup_database(
    db_url=f"sqlite:///{Path(Config.app_root).expanduser()}/results.db",
):
    engine = create_engine(db_url)

    @event.listens_for(engine, "connect")
    def enable_foreign_keys(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    Base.metadata.create_all(engine)
    return engine


class DBManager:
    def __init__(self, engine):
        self.Session = sessionmaker(bind=engine)

    def get_databases(
        self, id: Optional[int] = None, alias: Optional[str] = None
    ) -> list[Database]:
        """Return list of all database entries from metadb.

        Returns:
            list[Database]: list of Database objects.
        """
        with self.Session() as session:
            if id:
                return session.query(Database).filter_by(id=id).all()
            elif alias:
                return session.query(Database).filter_by(alias=alias).all()
            return session.query(Database).all()

    def add_database(
        self,
        db_type: str,
        host: str,
        port: str,
        user: str,
        password: str,
        dbname: str,
        alias: Optional[str] = None,
    ):
        """Add a database connection to metadb.

        Args:
            db_type (str): database type, supported: 'pg', 'mysql'
            host (str): hostname or IP address
            port (str): port number
            user (str): connection username
            password (str): connection password
            dbname (str): db schema (or database) name

        Raises:
            DatabaseError: any error occurred while executing SQL command.
        """
        try:
            with self.Session() as session:
                session.add(
                    Database(
                        db_type=db_type,
                        host=host,
                        port=port,
                        user=user,
                        password=password,
                        dbname=dbname,
                        alias=alias,
                    )
                )
                session.commit()
            logger.info("Added database connection.")
        except Exception as e:
            raise DatabaseError(None, None, e)

    def update_database(
        self,
        db_id: int,
        db_type: Optional[str] = None,
        host: Optional[str] = None,
        port: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None,
        dbname: Optional[str] = None,
        alias: Optional[str] = None,
    ):
        try:
            with self.Session() as session:
                db = session.query(Database).filter_by(id=db_id).first()
                if db:
                    if db_type:
                        db.db_type = db_type
                    if host:
                        db.host = host
                    if port:
                        db.port = port
                    if user:
                        db.user = user
                    if password:
                        db.password = password
                    if dbname:
                        db.dbname = dbname
                    if alias:
                        db.alias = alias
                    session.commit()
            logger.info(f"Updated database {db_id}.")
        except Exception as e:
            raise DatabaseError(None, None, e)

    def delete_database(self, db_id: Optional[int] = None, alias: Optional[str] = None):
        try:

            with self.Session() as session:
                if alias:
                    db = session.query(Database).filter_by(alias=alias).first()
                elif db_id:
                    db = session.query(Database).filter_by(id=db_id).first()
                if db:
                    session.delete(db)
                    session.commit()
            record = db_id if db_id else alias
            logger.info(f"Deleted database {record}.")
        except Exception as e:
            raise DatabaseError(None, None, e)

    def list_tables(self, db_id: Optional[int] = None, alias: Optional[str] = None):
        from .tpch.databases.base import Connection  # noqa: F401

        try:
            with self.Session() as session:
                if alias:
                    db = session.query(Database).filter_by(alias=alias).first()
                elif db_id:
                    db = session.query(Database).filter_by(id=db_id).first()
            if not db:
                raise RuntimeError("Database not found.")
            if db.db_type not in db_classes:
                raise ValueError(f"Unsupported database type: {db.db_type}")
            db_name = db.dbname

            # determine database type and dynamically import connection class
            dbconn: Connection
            db_class_path = db_classes[db.db_type]
            module_path, class_name = db_class_path.rsplit(".", 1)
            module = import_module(module_path, package="tpch_runner")
            db_class = getattr(module, class_name)

            dbconn = db_class(
                host=db.host,
                port=int(db.port),
                db_name=db.dbname,
                user=db.user,
                password=db.password,
            )

            if db.db_type == "pg":
                db_name = "public"
            stmt = f"SELECT table_name FROM information_schema.tables WHERE table_schema='{db_name}'"  # noqa: E501

            with dbconn as conn:
                conn.query(stmt)
                result = conn.fetch()

            return result
        except Exception as e:
            raise DatabaseError(None, None, e)


class TestResultManager:
    def __init__(self, engine: Engine):
        self.Session = sessionmaker(bind=engine)
        self._conn = engine.connect()

    @staticmethod
    def _generate_result_folder(db_type: str, time_value: datetime) -> str:
        current_time = time_value.strftime("%Y%m%d_%H%M%S")
        return f"{db_type}_{current_time}"

    def add_powertest(
        self, db_id: int, db_type: str, scale: str = "small", no_report: bool = False
    ) -> tuple[datetime, str]:
        attempt = 0
        max_attempts = 1
        while attempt < max_attempts:
            try:
                test_time = datetime.now()
                result_folder = self._generate_result_folder(db_type, test_time)

                if no_report:
                    logger.debug("generate test_time and result_folder.")
                    return test_time, result_folder
                with self.Session() as session:
                    session.add(
                        PowerTest(
                            testtime=test_time,
                            result_folder=result_folder,
                            db_type=db_type,
                            scale=scale,
                            database_id=db_id,
                        )
                    )
                    session.commit()
                logger.info(f"PowerTest added: {result_folder}")
            except Exception as e:
                self._conn.rollback()

                if attempt >= max_attempts:
                    print("Max attempts reached. Could not insert the record.")
                    raise DatabaseError(None, None, e)
                time.sleep(1)
            attempt += 1
        return test_time, result_folder

    def update_powertest(
        self,
        success: bool,
        runtime: float,
        test_id: Optional[int] = None,
        result_folder: Optional[str] = None,
    ):
        """
        Sets the runtime for a given PowerTest record after the test finishes.

        Parameters:
        result_folder (str): The identifier of the test to update.
        success (bool): If powertest succeed in all or not.
        runtime (float): The total runtime of the test.
        """
        try:
            with self.Session() as session:
                query = session.query(PowerTest)
                if test_id is not None:
                    power_test = query.filter_by(id=test_id).first()
                elif result_folder:
                    power_test = query.filter_by(result_folder=result_folder).first()

                if power_test is None:
                    raise ValueError(f"PowerTest {result_folder} not found.")

                power_test.success = success  # type: ignore
                power_test.runtime = runtime  # type: ignore

                session.commit()

            logger.info(
                "Test {} result updated: success={}, runtime={}s".format(
                    result_folder, success, runtime
                )
            )
        except Exception as e:
            raise DatabaseError(None, None, e)

    def update_powertest_comment(
        self, test_id: int, comment: Optional[str], scale: Optional[str]
    ):
        try:
            with self.Session() as session:
                power_test = session.query(PowerTest).filter_by(id=test_id).first()
                if power_test is None:
                    raise ValueError(f"PowerTest {test_id} not found.")

                power_test.comment = comment  # type: ignore
                power_test.scale = scale  # type: ignore

                session.commit()

            logger.info(f"PowerTest {test_id} updated.")
        except Exception as e:
            raise DatabaseError(None, None, e)

    def get_powertests(
        self,
        test_id: Optional[int] = None,
        db_type: Optional[str] = None,
        result_folder: Optional[str] = None,
    ) -> list[PowerTest]:
        with self.Session() as session:
            query = session.query(PowerTest).options(joinedload(PowerTest.results))
            if test_id is not None:
                query = query.filter(PowerTest.id == test_id)
            elif result_folder:
                query = query.filter(PowerTest.result_folder == result_folder)
            elif db_type:
                query = query.filter(PowerTest.db_type == db_type)
            return query.all()

    def delete_powertest(
        self, id: Optional[int] = None, result_folder: Optional[str] = None
    ):
        try:
            with self.Session() as session:
                query = session.query(PowerTest)
                if id:
                    query = query.filter(PowerTest.id == id)
                    result_folder = query.one().result_folder  # type: ignore
                    query = query.filter(PowerTest.result_folder == result_folder)

                if not result_folder:
                    raise RuntimeError("Result folder can't be None")

                query = query.filter(PowerTest.result_folder == result_folder)
                shutil.rmtree(RESULT_DIR.joinpath(result_folder))
                deleted_count = query.delete()
                session.commit()

            record = id if id else result_folder
            logger.info(f"Powertest record {record} deleted.")
            return deleted_count
        except Exception as e:
            raise e

    def compare_powertest(self, testid: int) -> tuple[bool, str]:
        all_pass: bool = True
        with self.Session() as session:
            pt_record = session.query(PowerTest).filter_by(id=testid).first()
            if pt_record is None:
                raise ValueError(f"PowerTest {testid} not found.")
            pt_folder: str = pt_record.result_folder  # type: ignore

            result = Result(
                db_type=pt_record.db_type,  # type: ignore
                result_dir=pt_folder,
                scale=pt_record.scale,  # type: ignore
            )
            for i in range(1, 22):
                result_file = f"{i}.csv"
                ok = result.compare_against_answer(result_file)
                if not ok:
                    all_pass = False
                    logger.error(
                        f"Query {i} result is not matched against answer. Test failed."
                    )
                else:
                    logger.info(f"Compare {result_file}: Good.")
            return all_pass, pt_folder

    def get_powertest_runtime(self, test_id: int) -> tuple[str, str, float, list[float]]:
        query_runtime: list[Column[float]] = []
        with self.Session() as session:
            query = session.query(PowerTest).options(joinedload(PowerTest.results))
            result = query.filter(PowerTest.id == test_id).first()
            if result is None:
                raise ValueError(f"PowerTest {test_id} not found.")

            result.results = sorted(result.results, key=lambda r: int(r.query_name[1:]))
            total_runtime = result.runtime
            db_type = result.db_type
            test_name = result.result_folder
            for record in result.results:
                query_runtime.append(record.runtime)
        return db_type, test_name, total_runtime, query_runtime  # type: ignore

    def add_test_result(
        self,
        db_type,
        success,
        rowcount,
        result_csv,
        query_name,
        runtime,
        result_folder,
        db_id,
    ):
        if rowcount is None:
            rowcount = 0
        try:
            with self.Session() as session:
                new_result = TestResult(
                    db_type=db_type,
                    success=success,
                    rowcount=rowcount,
                    result_csv=result_csv,
                    query_name=query_name,
                    runtime=runtime,
                    result_folder=result_folder,
                    database_id=db_id,
                )
                session.add(new_result)
                session.commit()
            logger.info(f"Test result added: {query_name} on {db_type}.")
        except Exception as e:
            raise e

    def get_test_results(self, db_type: Optional[str] = None) -> list[TestResult]:
        with self.Session() as session:
            query = session.query(TestResult).filter(TestResult.result_folder.is_(None))
            if db_type is not None:
                query = query.filter(TestResult.db_type == db_type)
            return query.all()

    def get_test_results_from_powertest(
        self,
        test_id: Optional[int] = None,
        result_folder: Optional[str] = None,
        db_type: Optional[str] = None,
    ) -> list[TestResult]:
        try:
            with self.Session() as session:
                query = session.query(TestResult).options(
                    joinedload(TestResult.power_test)
                )
                if test_id:
                    query = query.filter(TestResult.id == test_id)
                elif db_type:
                    query = query.filter(
                        TestResult.db_type == db_type,
                        TestResult.result_folder.isnot(None),
                    )
                elif result_folder:
                    query = query.filter(TestResult.result_folder == result_folder)
                else:
                    query = query.filter(TestResult.result_folder.isnot(None))
                return query.all()
        except Exception as e:
            raise e

    def read_result(self, test_id) -> tuple[TestResult, str, pd.DataFrame]:
        from .tpch.databases.base import TPCH_Runner

        query_dir = TPCH_Runner.query_dir
        try:
            result_detail = self.get_test_results_from_powertest(test_id=test_id).pop()
            if result_detail.power_test is not None:
                result_file = (
                    Path(RESULT_DIR)
                    .joinpath(result_detail.result_folder)
                    .joinpath(result_detail.result_csv)
                )
            else:
                result_file = Path(RESULT_DIR).joinpath(result_detail.result_csv)
            if result_file.exists():
                result_df = pd.read_csv(result_file)
            else:
                raise FileNotFoundError(f"Result file {result_file} not found.")

            query_str = query_dir.joinpath(f"{result_detail.query_name}.sql").read_text()
            return result_detail, query_str, result_df
        except IndexError:
            raise ValueError(f"No test result found for this test ID {test_id}.")

    def delete_test_result(self, test_id):
        try:
            with self.Session() as session:
                query = session.query(TestResult)
                if test_id:
                    query = query.filter(TestResult.id == test_id)
                record = query.first()
                if not record:
                    raise ValueError(f"Test result with ID {test_id} not found.")
                if record.result_folder and record.power_test.id:
                    raise ValueError(
                        "Can't delete individual test result associated with powertest.\n"
                        "Delete powertest to delete the entire test set."
                    )

                result_file = Path(RESULT_DIR).joinpath(record.result_csv)
                if result_file.exists():
                    result_file.unlink()
                deleted_count = query.delete()
                session.commit()
            logger.info(f"Test result {test_id} deleted.")
            return deleted_count
        except Exception as e:
            raise e
