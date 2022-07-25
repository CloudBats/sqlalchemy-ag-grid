import os

import sqlalchemy.orm

from sqlalchemy_ag_grid import AgGridQuery

CONN_URI = os.environ.get("DB_URL")
engine = sqlalchemy.create_engine(CONN_URI, pool_pre_ping=True)
AgGridSession = sqlalchemy.orm.sessionmaker(autocommit=False, autoflush=False, bind=engine, query_cls=AgGridQuery)
