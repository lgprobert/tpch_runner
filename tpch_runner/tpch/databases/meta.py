import shutil
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
    event,
)
from sqlalchemy.exc import DatabaseError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import joinedload, relationship, sessionmaker

from tpch_runner import logger
from tpch_runner.config import app_root

from .. import RESULT_DIR
from .results import Result

Base = declarative_base()


class TestResult(Base):  # type: ignore
    __tablename__ = "results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    testtime = Column(DateTime, default=datetime.utcnow, nullable=False)
    db_type = Column(String, nullable=False)
    # scale = Column(String, nullable=False)
    success = Column(Boolean, nullable=False)
    rowcount = Column(Integer, nullable=False)
    result_csv = Column(String, nullable=False)
    query_name = Column(String, nullable=False)
    runtime = Column(Float, nullable=False, default=0)
    result_folder = Column(
        String, ForeignKey("powertests.result_folder", ondelete="CASCADE")
    )
    # power_test = relationship("PowerTest", backref="results")
    power_test = relationship("PowerTest", backref="results", passive_deletes=True)


class PowerTest(Base):  # type: ignore
    __tablename__ = "powertests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    db_type = Column(String, nullable=False)
    scale = Column(String, nullable=False)
    result_folder = Column(String, unique=True, nullable=False)
    testtime = Column(DateTime, default=datetime.utcnow, nullable=False)
    success = Column(Boolean, nullable=True)
    runtime = Column(Float, default=0, nullable=True)
    # test_results = relationship("TestResult", backref="power_test")


def setup_database(db_url=f"sqlite:///{Path(app_root).expanduser()}/results.db"):
    logger.info("DB URL: %s", db_url)
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine


class TestResultManager:
    def __init__(self, engine):
        self.Session = sessionmaker(bind=engine)

        @event.listens_for(engine, "connect")
        def set_foreign_keys_on(dbapi_connection, connection_record):
            dbapi_connection.execute("PRAGMA foreign_keys = ON;")

    def add_powertest(
        self, testtime: datetime, result_folder: str, db_type: str, scale: str = "small"
    ):
        try:
            with self.Session() as session:
                session.add(
                    PowerTest(
                        testtime=testtime,
                        result_folder=result_folder,
                        db_type=db_type,
                        scale=scale,
                    )
                )
                session.commit()
        except Exception as e:
            raise DatabaseError(None, None, e)

    def update_powertest(self, test_id: str, success: bool, runtime: float):
        """
        Sets the runtime for a given PowerTest record after the test finishes.

        Parameters:
        test_id (str): The identifier of the test to update.
        success (bool): If powertest succeed in all or not.
        runtime (float): The total runtime of the test.
        """
        try:
            with self.Session() as session:
                power_test = (
                    session.query(PowerTest).filter_by(result_folder=test_id).first()
                )

                if power_test is None:
                    raise ValueError(f"PowerTest with test_id {test_id} not found.")

                power_test.success = success
                power_test.runtime = runtime

                session.commit()

                print(
                    "Test {} result updated: success={}, runtime={}s".format(
                        test_id, success, runtime
                    )
                )
        except Exception as e:
            raise DatabaseError(None, None, e)

    def get_powertests(
        self, db_type: Optional[str] = None, result_folder: Optional[str] = None
    ):
        session = self.Session()
        try:
            query = session.query(PowerTest)
            if result_folder:
                query = query.filter(PowerTest.result_folder == result_folder)
            elif db_type:
                query = query.filter(PowerTest.db_type == db_type)
            return query.all()
        finally:
            session.close()

    def delete_powertest(
        self, id: Optional[int] = None, result_folder: Optional[str] = None
    ):
        try:
            with self.Session() as session:
                query = session.query(PowerTest)
                if id:
                    query = query.filter(PowerTest.id == id)
                    result_folder = query.one().result_folder
                    query = query.filter(PowerTest.result_folder == result_folder)
                # elif result_folder:

                if not result_folder:
                    raise RuntimeError("Result folder can't be None")

                query = query.filter(PowerTest.result_folder == result_folder)
                shutil.rmtree(RESULT_DIR.joinpath(result_folder))
                session.query(TestResult).filter(
                    TestResult.result_folder == result_folder
                ).delete()
                deleted_count = query.delete()
                session.commit()
                logger.info("Deleted.")
                return deleted_count
        except Exception as e:
            raise e

    def compare_powertest(self, testid: int):
        with self.Session() as session:
            power_test = session.query(PowerTest).filter_by(id=testid).first()
            pt_folder: str = power_test.result_folder

            result = Result(
                db_type=power_test.db_type,
                result_dir=pt_folder,
                scale=power_test.scale,
            )
            for i in range(1, 22):
                result_file = f"{i}.csv"
                logger.info(
                    f"Compare {result_file}: {result.compare_against_answer(result_file)}"
                )

    def add_test_result(
        self,
        db_type,
        success,
        rowcount,
        result_csv,
        query_name,
        runtime,
        result_folder,
    ):

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
                )
                session.add(new_result)
                session.commit()
        except Exception as e:
            raise e

    def get_test_results(
        self,
        db_type: Optional[str] = None,
        result_csv: Optional[str] = None,
    ) -> list[TestResult]:
        try:
            with self.Session() as session:
                query = session.query(TestResult)
                if result_csv is not None:
                    query = query.filter(TestResult.result_csv == result_csv)
                elif db_type is not None:
                    query = query.filter(
                        TestResult.db_type == db_type,
                        TestResult.result_folder.isnot(None),
                    )
                else:
                    query = query.filter(TestResult.result_folder.is_(None))
                return query.all()
        except Exception as e:
            raise e

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
        from .base import TPCH_Runner

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
                return deleted_count
        except Exception as e:
            raise e
