from datetime import datetime
from unittest.mock import MagicMock, patch

import matplotlib.pyplot as plt
import pytest  # noqa

from tpch_runner import meta
from tpch_runner.commands import utils
from tpch_runner.commands.utils import barchart, format_datetime, get_db, get_db_manager
from tpch_runner.tpch.databases import base


@pytest.fixture
def sample_data():
    return [i * 0.5 for i in range(1, 23)]  # Sample data for 22 queries


@pytest.fixture
def temp_file(tmp_path):
    return tmp_path / "test_chart.png"


@pytest.fixture
def mock_db_manager():
    """Fixture to mock DBManager."""
    mock_rm = MagicMock()
    return mock_rm


def test_format_datetime_today(mocker):
    """Test when datetime is today."""
    mock_now = datetime(2025, 2, 15, 14, 30, 0)  # Fixed current time
    mocker.patch("tpch_runner.commands.utils.datetime", autospec=True)
    utils.datetime.now.return_value = mock_now
    utils.datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

    test_time = datetime(2025, 2, 15, 10, 15, 30)  # Same day
    assert format_datetime(test_time) == "10:15:30"


def test_format_datetime_different_day(mocker):
    """Test when datetime is from a different day."""
    mock_now = datetime(2025, 2, 15, 14, 30, 0)  # Fixed current time
    mocker.patch("tpch_runner.commands.utils.datetime", autospec=True)
    utils.datetime.now.return_value = mock_now
    utils.datetime.side_effect = lambda *args, **kwargs: datetime(*args, **kwargs)

    test_time = datetime(2025, 2, 10, 8, 45, 0)  # Different day
    assert format_datetime(test_time) == "2025-02-10"


def test_get_db_by_id(mock_db_manager):
    """Test retrieving a database by ID."""
    mock_db = MagicMock(id=1, alias="test_db", db_type="mysql")
    mock_db_manager.get_databases.return_value = [mock_db]

    db = get_db(mock_db_manager, id=1)

    assert db == mock_db
    mock_db_manager.get_databases.assert_called_once_with(id=1, alias=None)


def test_get_db_by_alias(mock_db_manager):
    """Test retrieving a database by alias."""
    mock_db = MagicMock(id=2, alias="test_db", db_type="pg")
    mock_db_manager.get_databases.return_value = [mock_db]

    db = get_db(mock_db_manager, alias_="test_db")

    assert db == mock_db
    mock_db_manager.get_databases.assert_called_once_with(id=None, alias="test_db")


def test_get_db_missing_id_and_alias(mocker, mock_db_manager):
    """Test when both ID and alias are missing."""
    mock_exit = mocker.patch("sys.exit")

    get_db(mock_db_manager)  # No ID or alias

    print(mock_exit.call_args_list)
    assert mock_exit.call_count >= 1
    mock_exit.assert_any_call(1)


def test_get_db_not_found(mocker, mock_db_manager):
    """Test when the database is not found."""
    mock_db_manager.get_databases.return_value = []  # Simulating not found
    mock_exit = mocker.patch("sys.exit")

    get_db(mock_db_manager, id=99)

    mock_exit.assert_called_once_with(1)


def test_get_db_unsupported_type(mocker, mock_db_manager):
    """Test when the database type is not supported."""
    mock_db = MagicMock(id=3, alias="unsupported_db", db_type="unsupported_db_type")
    mock_db_manager.get_databases.return_value = [mock_db]
    mocker.patch(
        "tpch_runner.commands.utils.meta.db_classes",
        {
            "mysql": ".tpch.databases.mysqldb.MySQLDB",
            "pg": ".tpch.databases.pgdb.PGDB",
            "rapidsdb": ".tpch.databases.rapidsdb.RapidsDB",
            "duckdb": ".tpch.databases.duckdb.DuckLDB",
        },
    )  # Simulated supported types
    mock_exit = mocker.patch("sys.exit")

    get_db(mock_db_manager, id=3)

    mock_exit.assert_called_once_with(1)


def test_get_db_index_error(mocker, mock_db_manager):
    """Test when get_databases() returns an empty list (IndexError)."""
    mock_db_manager.get_databases.return_value = []  # Simulating empty list
    mock_exit = mocker.patch("sys.exit")

    get_db(mock_db_manager, alias_="missing_db")

    mock_exit.assert_called_once_with(1)


def test_get_db_manager_mysql(mocker):
    db = meta.Database(
        db_type="mysql",
        host="localhost",
        port="3306",
        dbname="testdb",
        user="user",
        password="password",
        id=1,
    )

    mock_mysql_tpch = mocker.patch("tpch_runner.tpch.databases.mysqldb.MySQL_TPCH")
    mock_mysql_db = mocker.patch("tpch_runner.tpch.databases.mysqldb.MySQLDB")

    mock_mysql_instance = mocker.MagicMock(spec=base.TPCH_Runner)
    mock_mysql_tpch.return_value = mock_mysql_instance
    mock_mysql_db.return_value = mock_mysql_instance

    db_manager = get_db_manager(db)

    # Ensure the correct classes were instantiated
    mock_mysql_tpch.assert_called_once_with(mocker.ANY, db_id=db.id, scale="small")
    mock_mysql_db.assert_called_once_with(
        host=db.host,
        port=int(db.port),
        db_name=db.dbname,
        user=db.user,
        password=db.password,
    )

    assert isinstance(db_manager, base.TPCH_Runner)


def test_get_db_manager_pg(mocker):
    db = meta.Database(
        db_type="pg",
        host="localhost",
        port="5432",
        dbname="testdb",
        user="user",
        password="password",
        id=2,
    )

    mock_pg_tpch = mocker.patch("tpch_runner.tpch.databases.pgdb.PG_TPCH")
    mock_pg_db = mocker.patch("tpch_runner.tpch.databases.pgdb.PGDB")

    mock_pg_instance = mocker.MagicMock(spec=base.TPCH_Runner)
    mock_pg_tpch.return_value = mock_pg_instance
    mock_pg_db.return_value = mock_pg_instance

    db_manager = get_db_manager(db)

    mock_pg_tpch.assert_called_once_with(mocker.ANY, db_id=db.id, scale="small")
    mock_pg_db.assert_called_once_with(
        host=db.host,
        port=int(db.port),
        db_name=db.dbname,
        user=db.user,
        password=db.password,
    )

    assert isinstance(db_manager, base.TPCH_Runner)


def test_get_db_manager_rapidsdb(mocker):
    db = meta.Database(
        db_type="rapidsdb",
        host="localhost",
        port="4333",
        dbname="moxe",
        user="RAPIDS",
        password="rapids",
        id=3,
    )

    mock_rapidsdb_tpch = mocker.patch("tpch_runner.tpch.databases.rapidsdb.RDP_TPCH")
    mock_rapidsdb_db = mocker.patch("tpch_runner.tpch.databases.rapidsdb.RapidsDB")

    mock_rapidsdb_instance = mocker.MagicMock(spec=base.TPCH_Runner)
    mock_rapidsdb_tpch.return_value = mock_rapidsdb_instance
    mock_rapidsdb_db.return_value = mock_rapidsdb_instance

    db_manager = get_db_manager(db)

    mock_rapidsdb_tpch.assert_called_once_with(mocker.ANY, db_id=db.id, scale="small")
    mock_rapidsdb_db.assert_called_once_with(
        host=db.host,
        port=int(db.port),
        db_name=db.dbname,
        user=db.user,
        password=db.password,
    )

    assert isinstance(db_manager, base.TPCH_Runner)


def test_get_db_manager_duckdb(mocker):
    db = meta.Database(
        db_type="duckdb",
        host="localhost",
        port="~/data/duckdb/duckdb.db",
        id=4,
    )

    mock_duckdb_tpch = mocker.patch("tpch_runner.tpch.databases.duckdb.Duckdb_TPCH")
    mock_duckdb_db = mocker.patch("tpch_runner.tpch.databases.duckdb.DuckLDB")

    mock_duckdb_instance = mocker.MagicMock(spec=base.TPCH_Runner)
    mock_duckdb_tpch.return_value = mock_duckdb_instance
    mock_duckdb_db.return_value = mock_duckdb_instance

    db_manager = get_db_manager(db)

    mock_duckdb_tpch.assert_called_once_with(mocker.ANY, db_id=db.id, scale="small")
    mock_duckdb_db.assert_called_once_with(
        host=db.host,
        port=db.port,
        db_name=db.dbname,
        user=db.user,
        password=db.password,
    )

    assert isinstance(db_manager, base.TPCH_Runner)


def test_get_db_manager_unsupported_db_type():
    db = meta.Database(
        db_type="unsupporteddb",
        host="localhost",
        port="1234",
        dbname="testdb",
        user="user",
        password="password",
        id=5,
    )

    with pytest.raises(ValueError, match="Unsupported database type: unsupporteddb"):
        get_db_manager(db)


def test_get_db_manager_default_scale(mocker):
    db = meta.Database(
        db_type="mysql",
        host="localhost",
        port="3306",
        dbname="testdb",
        user="user",
        password="password",
        id=6,
    )

    mock_mysql_tpch = mocker.patch("tpch_runner.tpch.databases.mysqldb.MySQL_TPCH")
    mock_mysql_db = mocker.patch("tpch_runner.tpch.databases.mysqldb.MySQLDB")

    mock_mysql_instance = mocker.MagicMock(spec=base.TPCH_Runner)
    mock_mysql_tpch.return_value = mock_mysql_instance
    mock_mysql_db.return_value = mock_mysql_instance

    db_manager = get_db_manager(db)  # No scale argument, should default to "small"

    mock_mysql_tpch.assert_called_once_with(mocker.ANY, db_id=db.id, scale="small")
    mock_mysql_db.assert_called_once_with(
        host=db.host,
        port=int(db.port),
        db_name=db.dbname,
        user=db.user,
        password=db.password,
    )

    assert isinstance(db_manager, base.TPCH_Runner)


def test_barchart_creates_plot(sample_data, temp_file):
    """Test if barchart function creates a valid plot and saves it."""
    with patch("matplotlib.pyplot.savefig") as mock_savefig:
        barchart("Test Chart", sample_data, temp_file)

        # Ensure savefig was called with the correct filename
        mock_savefig.assert_called_once_with(temp_file, dpi=300)


def test_barchart_labels(sample_data, temp_file):
    """Test if the chart uses the correct labels."""
    with patch("matplotlib.pyplot.savefig"):
        barchart("Test Chart", sample_data, temp_file)

    # Check if the correct labels are used
    expected_labels = [f"Q{i}" for i in range(1, 23)]
    actual_labels = [tick.get_text() for tick in plt.gca().get_xticklabels()]
    assert expected_labels == actual_labels, "X-axis labels are incorrect"


def test_barchart_no_legend(sample_data, temp_file):
    """Test that the legend is empty since no labels are provided."""
    with patch("matplotlib.pyplot.savefig"):
        barchart("Test Chart", sample_data, temp_file)

    legend = plt.gca().get_legend()

    assert legend is not None, "Legend should exist but be empty."
    assert len(legend.get_texts()) == 0, "Legend should have no labels but does."


def test_barchart_handles_empty_data(temp_file):
    """Test that barchart raises ValueError when data is empty."""
    with pytest.raises(
        ValueError, match="data can't be empty and must have 22 elements."
    ):
        barchart("Test Chart", [], temp_file)


def test_barchart_handles_different_lengths(temp_file):
    """Test if the function handles incorrect data lengths properly."""
    wrong_data = [1, 2, 3]  # Less than 22 values
    with pytest.raises(
        ValueError, match="data can't be empty and must have 22 elements."
    ):
        barchart("Wrong Data Chart", wrong_data, temp_file)
