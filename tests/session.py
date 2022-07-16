import sqlalchemy.orm
from contextlib import contextmanager
from sqlalchemy_ag_grid import AgGridQuery

# TODO: Extract this as a environment variable that would be used in GitHub Actions.
CONN_URI = 'postgresql://postgres:postgres@localhost:5432/postgres'
engine = sqlalchemy.create_engine(CONN_URI, pool_pre_ping=True)
AgGridSession = sqlalchemy.orm.sessionmaker(autocommit=False, autoflush=False, bind=engine, query_cls=AgGridQuery)


# TODO: move this to conftest.py once nose2 is replaced with pytest
@contextmanager
def db_session():
    session_ = None
    try:
        session_ = AgGridSession()
        yield session_
    finally:
        if session_ is not None:
            session_.close()
