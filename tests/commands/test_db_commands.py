import logging
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from click.testing import CliRunner

from tpch_runner import meta
from tpch_runner.commands import db_commands


@pytest.fixture
def mock_db_manager():
    """Fixture to mock DBManager and the get_databases method."""
    mock_rm = MagicMock(spec=meta.DBManager)

    mock_rm.get_databases.return_value = [
        meta.Database(
            id=1,
            db_type="PostgreSQL",
            host="localhost",
            port=5432,
            user="user1",
            dbname="db1",
            alias="DB1",
        ),
        meta.Database(
            id=2,
            db_type="MySQL",
            host="localhost",
            port=3306,
            user="user2",
            dbname="db2",
            alias="DB2",
        ),
    ]

    return mock_rm


@pytest.mark.parametrize(
    "visible_command",
    [
        "add",
        "count",
        "create",
        "delete",
        "drop",
        "list",
        "load",
        "reload",
        "tables",
        "truncate",
        "update",
    ],
)
def test_visible_command(visible_command):
    """Test visible commands in help message"""
    runner = CliRunner()
    result = runner.invoke(db_commands.cli, ["--help"])
    print(result.output)
    assert result.exit_code == 0
    assert visible_command in result.output


def test_ls_command(mock_db_manager):
    """Test the 'list' command of the DB CLI."""
    runner = CliRunner()

    result = runner.invoke(
        db_commands.cli,
        args=["list"],
        obj={"rm": mock_db_manager},
    )

    assert result.exit_code == 0
    for header in ["Type", "Host", "Port", "User", "DBName", "Alias"]:
        assert header in result.output
    assert "DB1" in result.output
    assert "DB2" in result.output


def test_ls_db_manager_raises_exception(mocker, caplog):
    """Test when DBManager.get_databases() raises an exception."""
    runner = CliRunner()

    mock_rm = MagicMock()
    mock_rm.get_databases.side_effect = RuntimeError("Database connection failed")

    with caplog.at_level(logging.ERROR):
        result = runner.invoke(db_commands.cli, ["list"], obj={"rm": mock_rm})

    assert result.exit_code != 0  # Ensure the command fails
    assert "Error: " in caplog.text


def test_add_valid_db(mocker, mock_db_manager):
    """Test adding a valid database."""
    runner = CliRunner()
    result = runner.invoke(
        db_commands.cli,
        [
            "add",
            "-t",
            "mysql",
            "-H",
            "localhost",
            "-p",
            "3306",
            "-u",
            "user",
            "-d",
            "testdb",
            "-a",
            "TestDB",
        ],
        obj={"rm": mock_db_manager},
    )

    assert result.exit_code == 0
    assert "Database added successfully." in result.output
    mock_db_manager.add_database.assert_called_once_with(
        "mysql", "localhost", "3306", "user", "", "testdb", "TestDB"
    )


def test_add_port_as_dbfile(mocker, mock_db_manager):
    """Test adding a database with a non-numeric port (e.g., duckdb file path)."""
    runner = CliRunner()
    invalid_port = "~/database.db"
    expanded_port = str(Path(invalid_port).expanduser())

    result = runner.invoke(
        db_commands.cli,
        ["add", "-t", "duckdb", "-p", invalid_port],
        obj={"rm": mock_db_manager},
    )

    assert result.exit_code == 0
    mock_db_manager.add_database.assert_called_once_with(
        "duckdb", "localhost", expanded_port, "", "", "", None
    )


def test_add_db_manager_exception(mocker, mock_db_manager, caplog):
    """Test when DBManager raises an exception."""
    mock_db_manager.add_database.side_effect = RuntimeError("Database error")

    runner = CliRunner()
    with caplog.at_level(logging.ERROR):
        result = runner.invoke(
            db_commands.cli,
            ["add", "-t", "mysql", "-p", "3306"],
            obj={"rm": mock_db_manager},
        )

    assert result.exit_code != 0
    assert "Error: " in caplog.text
    mock_db_manager.add_database.assert_called_once()


def test_add_password_prompt(mocker, mock_db_manager):
    """Test entering a password via the CLI prompt."""
    runner = CliRunner()
    mocker.patch("click.prompt", return_value="securepassword")

    result = runner.invoke(
        db_commands.cli,
        ["add", "-t", "mysql", "-p", "3306", "-W"],
        obj={"rm": mock_db_manager},
    )

    assert result.exit_code == 0
    assert "Database added successfully." in result.output
    mock_db_manager.add_database.assert_called_once_with(
        "mysql", "localhost", "3306", "", "securepassword", "", None
    )


def test_delete_success_by_id(mock_db_manager):
    """Test deleting a database using its ID."""
    runner = CliRunner()
    result = runner.invoke(
        db_commands.cli,
        ["delete", "1"],
        obj={"rm": mock_db_manager},
    )

    assert result.exit_code == 0
    assert "Database deleted successfully." in result.output
    mock_db_manager.delete_database.assert_called_once_with(db_id=1, alias=None)


def test_delete_success_by_alias(mock_db_manager):
    """Test deleting a database using its alias."""
    runner = CliRunner()
    result = runner.invoke(
        db_commands.cli,
        ["delete", "-a", "test_db"],  # Using alias instead of ID
        obj={"rm": mock_db_manager},
    )

    assert result.exit_code == 0
    assert "Database deleted successfully." in result.output
    mock_db_manager.delete_database.assert_called_once_with(db_id=None, alias="test_db")
