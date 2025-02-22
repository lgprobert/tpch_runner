import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker

from tpch_runner import meta


@pytest.fixture
def session():
    engine = create_engine("sqlite:///:memory:")

    @event.listens_for(engine, "connect")
    def enable_foreign_keys(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.close()

    meta.Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    return Session()
