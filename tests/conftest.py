import pytest

from .base_model import Base
from .data import data
from .item import Item
from .session import AgGridSession, engine


@pytest.fixture
def db_session():
    session_ = None
    try:
        session_ = AgGridSession()
        yield session_
    finally:
        if session_ is not None:
            session_.close()


@pytest.fixture
def insert_data(db_session):
    db_session.execute(Item.__table__.insert(), data)
    db_session.commit()


@pytest.fixture(autouse=True)
def refresh_db():
    Base.metadata.drop_all(bind=engine)
    Base.metadata.create_all(bind=engine)
