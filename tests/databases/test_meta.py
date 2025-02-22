from itertools import cycle
from unittest.mock import MagicMock, create_autospec, patch

import pytest
from sqlalchemy import inspect
from sqlalchemy.orm import Session

from tpch_runner import meta


def test_tables_exist(session):
    """Ensure tables are created successfully."""
    inspector = inspect(session.bind)
    tables = inspector.get_table_names()

    assert "databases" in tables
    assert "results" in tables
    assert "powertests" in tables


def test_cascade_delete(session):
    db = meta.Database(
        db_type="mysql",
        alias="test_db",
        host="localhost",
        port="3306",
        user="test_user",
        password="test_pass",
        dbname="test_dbname",
        version="8.0",
        config="default",
        scale="small",
        description="Test database",
    )
    session.add(db)
    session.commit()  # Ensure existence

    # Step 2: Create and commit a PowerTest entry linked to the Database
    power_test = meta.PowerTest(
        db_type="mysql",
        scale="large",
        result_folder="test123",
        database_id=db.id,  # Correct reference
    )
    session.add(power_test)
    session.commit()

    # Step 3: Add and commit a TestResult linked to PowerTest
    test_result = meta.TestResult(
        database_id=db.id,  # Correct reference
        power_test=power_test,
        db_type="mysql",
        rowcount=100,
        result_csv="test.csv",
        success=True,
        query_name="Q1",
        runtime=10.5,
        result_folder="test123",
    )
    session.add(test_result)
    session.commit()

    # Verify TestResult exists
    assert session.query(meta.TestResult).count() == 1

    # Step 4: Delete PowerTest (should cascade delete TestResult)
    session.delete(power_test)
    session.commit()

    # Step 5: Verify TestResult is deleted
    assert session.query(meta.TestResult).count() == 0


def test_default_values(session):
    pg = meta.Database(
        id=2,
        db_type="pg",
        alias="test_db",
        host="localhost",
        port="5432",
        user="test_user",
        password="test_pass",
        dbname="test_dbname",
        version="14",
        config="default",
        scale="small",
        description="PG Test database",
    )
    session.add(pg)
    session.commit()

    power_test = meta.PowerTest(
        db_type="pg", scale="small", result_folder="test321", database_id=2
    )
    session.add(power_test)
    session.commit()

    assert power_test.runtime == 0  # Default value
    assert power_test.testtime is not None


class Test_TestResultManager:
    # @pytest.fixture
    # def mock_session(self, mocker):
    #     # Mock the Session object in TestResultManager
    #     mock_session = mocker.patch("tpch_runner.meta.TestResultManager.Session")
    #     # Mock the query method of the session
    #     mock_query = mocker.patch.object(mock_session, "query")
    #     return mock_query

    def test_compare_powertest_not_found(self, session):
        mock_session = create_autospec(Session)
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter_by.return_value.first.return_value = None

        pt_class = meta.TestResultManager(session.bind)
        pt_class.Session = MagicMock(return_value=mock_session)

        with pytest.raises(ValueError, match="PowerTest 123 not found."):
            pt_class.compare_powertest(123)

    def test_compare_powertest_success(self, mocker, session):
        """Test sucessful comparison."""
        pt_record = meta.PowerTest(
            id=1, db_type="mysql", result_folder="test_folder", scale="small"
        )

        mock_compare = mocker.patch.object(
            meta.Result, "compare_against_answer", return_value=True
        )

        mock_session = create_autospec(Session)
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter_by.return_value.first.return_value = (
            pt_record
        )

        mocker.patch("tpch_runner.meta.sessionmaker", return_value=mock_session)

        pt_class = meta.TestResultManager(session.bind)
        pt_class.Session = MagicMock(return_value=mock_session)

        all_pass, pt_folder = pt_class.compare_powertest(1)

        assert all_pass is True
        assert pt_folder == "test_folder"
        mock_compare.assert_called_with("21.csv")

    def test_compare_powertest_failure(self, mocker, session):
        """Test compare_powertest() returns False when some unmatched results found."""
        pt_record = meta.PowerTest(
            id=1, db_type="mysql", result_folder="test_folder", scale="small"
        )

        mock_session = create_autospec(Session)
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter_by.return_value.first.return_value = (
            pt_record
        )
        mocker.patch("tpch_runner.meta.sessionmaker", return_value=mock_session)

        mock_compare = mocker.patch.object(meta.Result, "compare_against_answer")
        mock_compare.side_effect = cycle([False, True])

        pt_class = meta.TestResultManager(session.bind)
        pt_class.Session = MagicMock(return_value=mock_session)

        all_pass, pt_folder = pt_class.compare_powertest(1)

        assert all_pass is False
        assert pt_folder == "test_folder"
        mock_compare.assert_any_call("21.csv")

    def test_compare_powertest_comparison_calls(self, mocker, session):
        """Test correct number of results are compared."""
        pt_record = meta.PowerTest(
            id=1, db_type="mysql", result_folder="test_folder", scale="small"
        )
        mock_session = create_autospec(Session)
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter_by.return_value.first.return_value = (
            pt_record
        )
        mocker.patch("tpch_runner.meta.sessionmaker", return_value=mock_session)
        mock_compare = mocker.patch.object(
            meta.Result, "compare_against_answer", return_value=True
        )

        pt_class = meta.TestResultManager(session.bind)
        pt_class.Session = MagicMock(return_value=mock_session)

        pt_class.compare_powertest(1)

        for i in range(1, 22):
            mock_compare.assert_any_call(f"{i}.csv")

    def test_compare_powertest_logging_on_failure(self, mocker, session, caplog):
        """Test log message when unmatched result found."""
        pt_record = meta.PowerTest(
            id=1, db_type="mysql", result_folder="test_folder", scale="small"
        )
        mock_session = create_autospec(Session)
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter_by.return_value.first.return_value = (
            pt_record
        )
        mocker.patch("tpch_runner.meta.sessionmaker", return_value=mock_session)

        mock_compare = mocker.patch.object(meta.Result, "compare_against_answer")
        mock_compare.side_effect = cycle([False, True])

        pt_class = meta.TestResultManager(session.bind)
        pt_class.Session = MagicMock(return_value=mock_session)
        pt_class.compare_powertest(1)

        err = "Query 1 result is not matched against answer. Test failed."
        assert err in caplog.text

    def test_compare_powertest_compare_exception(self, mocker, session):
        """Test exception occurred from compare_against_answer() method."""
        pt_record = meta.PowerTest(
            id=1, db_type="mysql", result_folder="test_folder", scale="small"
        )
        mock_session = create_autospec(Session)
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter_by.return_value.first.return_value = (
            pt_record
        )
        mocker.patch("tpch_runner.meta.sessionmaker", return_value=mock_session)

        mock_compare = mocker.patch.object(meta.Result, "compare_against_answer")
        mock_compare.side_effect = Exception("Comparison failed")

        pt_class = meta.TestResultManager(session.bind)
        pt_class.Session = MagicMock(return_value=mock_session)

        with patch("tpch_runner.meta.Result.compare_against_answer") as mock_compare:
            mock_compare.side_effect = FileNotFoundError("Answer folder not exists:")
            with pytest.raises(FileNotFoundError, match="Answer folder not exists:"):
                pt_class.compare_powertest(1)

    def test_compare_powertest_nonexisting_record(self, mocker, session):
        """Test non-existing powertest record."""
        mock_session = create_autospec(Session)
        mock_session.__enter__.return_value = mock_session
        mock_session.query.return_value.filter_by.return_value.first.return_value = None
        mocker.patch("tpch_runner.meta.sessionmaker", return_value=mock_session)

        pt_class = meta.TestResultManager(session.bind)
        pt_class.Session = MagicMock(return_value=mock_session)

        with pytest.raises(ValueError, match="PowerTest 1 not found"):
            pt_class.compare_powertest(1)
