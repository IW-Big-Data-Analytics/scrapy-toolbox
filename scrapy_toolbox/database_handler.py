import os 

from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy_utils import database_exists, create_database

from typing import Final


class DatabaseHandler:
    def __init__(self, database_credentials: dict[str, str]) -> None:
        self.engine: Final[Engine] = create_engine(URL(**database_credentials))

        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        DeclarativeBase.metadata.create_all(self.engine, checkfirst=True)


        # self.insert_module = importlib.import_module(f'sqlalchemy.dialects.{sqltype}')


    def store_items(self, items: dict[Base]):
        pass