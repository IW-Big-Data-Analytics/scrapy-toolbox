import os
 
from scrapy import signals
from sqlalchemy.inspection import inspect
from .mapper import ItemsModelMapper
from typing import Final, Type

from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database

DeclarativeBase = declarative_base()

# https://www.python.org/download/releases/2.2/descrintro/#__new__
class Singleton(object):
    def __new__(cls, *args, **kwds):
        it = cls.__dict__.get("__it__")
        if it is not None:
            return it
        cls.__it__ = it = object.__new__(cls)
        it.init(*args, **kwds)
        return it

    def init(self, *args, **kwds):
        pass


class DatabasePipeline():
    def __init__(self, settings, items=None, model=None):
        database_credentials = settings.get('DATABASE') if'PRODUCTION' in os.environ else settings.get('DATABASE_DEV')

        if database_credentials:
            self.engine: Final[Engine] = create_engine(URL(**database_credentials))
        else:
            raise AttributeError('Could not find database credentials in settings.')

        if not database_exists(self.engine.url):
            create_database(self.engine.url)
        DeclarativeBase.metadata.create_all(self.engine, checkfirst=True)

        if items and model:
           self.mapper = ItemsModelMapper(items=items, model=model)

        self.items: Final[dict[Type, list]] = {}
        self.item_counter: int = 0

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        crawler.database_engine = pipeline.engine
        return pipeline

    # def create_session(self, engine):
    #     session = sessionmaker(bind=engine, autoflush=False)() # autoflush=False: "This is useful when initializing a series of objects which involve existing database queries, where the uncompleted object should not yet be flushed." for instance when using the Association Object Pattern
    #     return session

    def spider_closed(self, spider):
        if self.item_counter > 0:
            self.insert_into_db(self.items)


    def process_item(self, item, spider):
        model_for_item = self.mapper.get_model(item=item) 
        self.items.setdefault(model_for_item.__table__, []).append(item)
        self.item_counter += 1
        if self.item_counter >= 500:
            self.insert_into_db(self.items)
            self.item_counter = 0
            self.items = {}
        
        
    def insert_into_db(self, items):
        with Session(bind=self.engine) as session:
            session.add_all(items)
            session.commit()