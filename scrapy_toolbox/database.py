import os
 
from scrapy import signals
from sqlalchemy.inspection import inspect
from .mapper import ItemsModelMapper
from typing import Final, Type

from sqlalchemy import Table, create_engine, ForeignKey, select
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

        self.existing_model_items: Final[dict[Type, set]] = {}
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
        model_item = self.mapper.map_to_model(item)
        self.insert_into_db(model_item)
        
        
    def insert_into_db(self, model_item: Type):
        with Session(bind=self.engine) as session:
            for relationship in inspect(model_item.__class__).relationships:
                rel_name: Final[str] = relationship.key
                rel_obj: Final[Type] = model_item.__getattribute__(rel_name)


                #checking for existence
                object_exists: Final[bool] = False


                if not object_exists:
                    self.inset_into_db(rel_obj)



        #     print('hallo')
        # pass 





        # with Session(bind=self.engine) as session:
        #         model_item = self.mapper.get_model(item)
        #         existing_model_items: Final[set] = self.existing_model_items[model_item.__table__]
        #         #check if exists in existin
        #         item_already_queried: Final[bool] = model_item in existing_model_items

        #         if not item_already_queried:
                    # existing_model_item = select(model_item.__table__)

                    #  if not query
                        # add to existing
                        # add to session
                            # add to existing
                    
                #commit added items

    def get_foreign_key_values(self, model_item) -> dict:
        #find foreign key classes
        #check if values for foreign keys exist
            # return value of column
        #if not, add item
        #return value of foreign key column
        with Session(bind=self.engine) as session:
            for foreign_key in model_item.__table__.foreign_keys:
                fk_table: Final[Table] = foreign_key.column.table
                fk_col_name: Final[str] = foreign_key.name
            
        pass