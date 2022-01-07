from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from sqlalchemy import String, Float, Integer
from sqlalchemy.sql.schema import ForeignKey
from typing import Final

Base = declarative_base()

class Name(Base):
    __tablename__: Final[str] = 'names'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)


class Hometown(Base):
    __tablename__: Final[str] = 'hometowns'

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(255), unique=True, nullable=False)
    population = Column(Float)


class Person(Base):
    __tablename__: Final[str] = 'persons'

    name_id = Column(Integer, ForeignKey(Name.id), primary_key=True)
    hometown_id = Column(Integer, ForeignKey(Hometown.id), primary_key=True)
    weight = Column(Float, primary_key=True)
    height = Column(Float, primary_key=True)
    shirt_color = Column(String(255), primary_key=True)

