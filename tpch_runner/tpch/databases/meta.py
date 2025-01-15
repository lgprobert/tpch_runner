from datetime import datetime
from typing import Optional

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    String,
    create_engine,
)
from sqlalchemy.exc import DatabaseError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker

Base = declarative_base()


class TestResult(Base):  # type: ignore
    __tablename__ = "results"

    id = Column(Integer, primary_key=True, autoincrement=True)
    testtime = Column(DateTime, default=datetime.utcnow, nullable=False)
    db_type = Column(String, nullable=False)
    scale = Column(String, nullable=False)
    success = Column(Boolean, nullable=False)
    rowcount = Column(Integer, nullable=False)
    result_csv = Column(String, nullable=False)
    query_name = Column(String, nullable=False)
    runtime = Column(Float, nullable=False, default=0)
    powertest_folder = Column(Integer, ForeignKey("powertests.id"), nullable=True)


class PowerTest(Base):  # type: ignore
    __tablename__ = "powertests"

    id = Column(Integer, primary_key=True, autoincrement=True)
    db_type = Column(String, nullable=False)
    scale = Column(String, nullable=False)
    result_folder = Column(String, unique=True, nullable=False)
    testtime = Column(DateTime, default=datetime.utcnow, nullable=False)
    success = Column(Boolean, nullable=True)
    runtime = Column(Float, default=0, nullable=True)
    test_results = relationship("TestResult", backref="power_test")


def setup_database(db_url="sqlite:///results.db"):
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine


class TestResultManager:
    def __init__(self, engine):
        self.Session = sessionmaker(bind=engine)

    def add_powertest(self, testtime: datetime, result_folder: str):
        try:
            with self.Session() as session:
                session.add(PowerTest(testtime=testtime, result_folder=result_folder))
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

    def delete_powertest(self, result_folder):
        try:
            with self.Session() as session:
                query = session.query(PowerTest)
                if result_folder:
                    query = query.filter(PowerTest.id == result_folder)
                deleted_count = query.delete()
                session.commit()
                return deleted_count
        except Exception as e:
            raise e

    def add_test_result(
        self,
        db_type,
        success,
        rowcount,
        result_csv,
        query_name,
        runtime,
        powertest_folder,
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
                    powertest_folder=powertest_folder,
                )
                session.add(new_result)
                session.commit()
        except Exception as e:
            raise e

    def get_test_results(
        self, db_type=None, result_csv=None, powertest_folder: Optional[str] = None
    ):
        session = self.Session()
        try:
            query = session.query(TestResult)
            if result_csv is not None:
                query = query.filter(
                    TestResult.result_csv == result_csv
                    and TestResult.powertest_folder == powertest_folder
                )
            elif db_type is not None:
                query = query.filter(TestResult.db_type == db_type)
            elif powertest_folder is None:
                query = query.filter(TestResult.powertest_folder.is_(None))
            elif powertest_folder is not None:
                query = query.filter(TestResult.powertest_folder == powertest_folder)
            return query.all()
        finally:
            session.close()

    def delete_test_result(self, test_id):
        try:
            with self.Session() as session:
                query = session.query(TestResult)
                if test_id:
                    query = query.filter(TestResult.id == test_id)
                deleted_count = query.delete()
                session.commit()
                return deleted_count
        except Exception as e:
            raise e


# Example usage
if __name__ == "__main__":
    engine = setup_database()
    manager = TestResultManager(engine)

    # Add a test result
    manager.add_test_result(
        db_type="pg",
        success=True,
        rowcount=2,
        result_csv="pg_q14_2025-01-12-225609.csv",
        query_name="14",
        runtime=0,
        powertest_folder=None,
    )

    # Add a power test
    manager.add_powertest(testtime=datetime.now(), result_folder="mysql_20250113_013632")

    # Get all power tests
    power_tests = manager.get_powertests()
    for power_test in power_tests:
        print(power_test.id, power_test.datetime, power_test.result_folder)

    # Get all test results
    results = manager.get_test_results()
    for result in results:
        print(
            result.id,
            result.testtime,
            result.db_type,
            result.success,
            result.rowcount,
            result.result_csv,
            result.query_name,
        )

    # Get specific test results by query name
    specific_results = manager.get_test_results(result_csv="14")
    for result in specific_results:
        print(
            result.id,
            result.testtime,
            result.db_type,
            result.success,
            result.rowcount,
            result.result_csv,
            result.query_name,
        )

    # Delete test results
    deleted_count = manager.delete_test_result(test_id="1")
    print(f"Deleted {deleted_count} results.")
