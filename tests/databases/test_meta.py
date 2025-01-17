import unittest
from unittest.mock import MagicMock

from sqlalchemy import create_engine
from sqlalchemy.sql import text

from tpch_runner.tpch.databases.meta import Base, TestResultManager


class TestGetPowerTestRuntime(unittest.TestCase):
    def setUp(self):
        self.engine = create_engine("sqlite:///:memory:")
        Base.metadata.create_all(self.engine)
        self.manager = TestResultManager(self.engine)

        self.mock_session = MagicMock()

        self.test_id = 1

    def tearDown(self):
        Base.metadata.drop_all(bind=self.engine)
        self.engine.dispose()

    def test_get_powertest_runtime_valid(self):
        with self.engine.begin() as connection:
            connection.execute(
                text(
                    """
                INSERT INTO powertests
                (id, db_type, scale, result_folder, testtime, success, runtime)
                VALUES (1, 'pg', 'small', 'test_folder', '2000-01-01', 1, 120.5 )
            """
                )
            )
            connection.execute(
                text(
                    """
                INSERT INTO results
                    (id, db_type, result_folder, testtime, success, runtime,
                    rowcount, result_csv, query_name)
                VALUES (1, 'pg', 'test_folder', '2000-01-01', 1, 120.5, 10, '1.csv', '1')
            """
                )
            )

        obj = self.manager
        db_type, test_name, total_runtime, query_runtime = obj.get_powertest_runtime(
            self.test_id
        )

        self.assertEqual(db_type, "pg")
        self.assertEqual(test_name, "test_folder")
        self.assertEqual(total_runtime, 120.5)
        self.assertEqual(query_runtime, [120.5])


if __name__ == "__main__":
    unittest.main()
