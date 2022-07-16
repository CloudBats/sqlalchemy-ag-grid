import sqlalchemy as sa
from .base_model import Base


class Item(Base):
    __tablename__ = 'item'

    id = sa.Column(sa.Integer, primary_key=True)
    text1 = sa.Column(sa.String, nullable=False)
    text2 = sa.Column(sa.String, nullable=False)
    number1 = sa.Column(sa.Integer, nullable=False)
    number2 = sa.Column(sa.Integer, nullable=False)
