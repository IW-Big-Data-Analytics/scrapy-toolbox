import importlib
from scrapy import signals
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.inspection import inspect
from .mapper import ItemsModelMapper
from typing import Final, Type
import os

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


class DatabasePipeline(Singleton):
    def __init__(self, settings, items=None, model=None, database=None, database_dev=None, sqltype='mysql'):
        if database:
            self.database = database
        elif settings:
            self.database = settings.get("DATABASE")
            # self.database["query"]["charset"] = 'utf8mb4'
        if database_dev:
            self.database_dev = database_dev
        elif settings:
            self.database_dev = settings.get("DATABASE_DEV")
            # self.database_dev["query"]["charset"] = 'utf8mb4'
        self.engine = self.create_engine()
        if items and model:
            self.mapper = ItemsModelMapper(items=items, model=model)

        self.insert_module = importlib.import_module(f'sqlalchemy.dialects.{sqltype}')
        self.items: Final[dict[Type, list]] = {}
        self.item_counter: int = 0

    @classmethod
    def from_crawler(cls, crawler):
        pipeline = cls(crawler.settings)
        crawler.signals.connect(pipeline.spider_closed, signals.spider_closed)
        crawler.database_engine = pipeline.engine
        return pipeline

    def create_engine(self):
        if "PRODUCTION" in os.environ:
            engine = create_engine(URL(**self.database))
        else:
            engine = create_engine(URL(**self.database_dev))
        if not database_exists(engine.url):
            create_database(engine.url)
        DeclarativeBase.metadata.create_all(engine, checkfirst=True)
        return engine

    def create_tables(self, engine):
        DeclarativeBase.metadata.create_all(engine, checkfirst=True)

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
        for table in items:
            if len(self.items[table]) > 0: 
                primary_keys = [key.name for key in inspect(table).primary_key]
                with self.engine.connect() as conn:
                    stmt = self.insert_module.insert(table) \
                                             .values([{**item} for item in self.items[table]]) \
                                            #  .prefix_with("IGNORE")

                    try:
                        stmt = stmt.on_conflict_do_nothing(index_elements=primary_keys)
                    except AttributeError:
                        stmt = stmt.prefix_with("IGNORE")

                    conn.execute(stmt)