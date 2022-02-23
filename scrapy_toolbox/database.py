import os
 
from scrapy import signals
from sqlalchemy.inspection import inspect
from .mapper import ItemsModelMapper
from typing import Final, Type

from sqlalchemy import Table, create_engine, ForeignKey, Column, select
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy_utils import database_exists, create_database
from sqlalchemy.orm.relationships import RelationshipProperty
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.dialects.mysql import insert as mysql_insert

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
            if self.engine.name != 'mysql' and self.engine.name != 'postgresql':
                raise AttributeError('Only MySQL and PostgreSQL are supported as SQL-types.')
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

    def spider_closed(self, spider):
        if self.item_counter > 0:
            self.insert_into_db(self.items)


    def process_item(self, item, spider):
        self.persist_item(item)
    

    def persist_item(self, item, return_values: list[Column] = None):
        model_item = self.mapper.map_to_model(item)

        foreign_keys: Final[set[ForeignKey]] = model_item.__table__.foreign_keys

        for relationship in inspect(model_item.__class__).relationships:
            associated_fks = [fk for fk in foreign_keys if fk.column.table == relationship.target]
            needed_fk_values_present: Final[bool] = all(model_item.__dict__.get(fk.parent.description) is not None for fk in associated_fks)
            rel_name: Final[str] = relationship.key
            rel_item = model_item.__dict__.get(rel_name)

            if not needed_fk_values_present:
                if not rel_item: 
                        raise AttributeError('Item for relationship missing while associated foreign keys are not set.')
                else:
                    needed_column_values: Final[list[Column]] = [fk.column for fk in associated_fks]
                    rel_model_item = self.persist_item(rel_item, return_values=needed_column_values)
                    setattr(model_item, rel_name, rel_model_item)

                    for fk in associated_fks:
                        setattr(model_item, fk.parent.description, getattr(rel_model_item, fk.column.name))
        
        return self.insert_into_db(model_item, return_values=return_values)
        
        
    def insert_into_db(self, model_item: Type, return_values: list[Column]) -> Type:
        with Session(bind=self.engine) as session:
            col_val_mapping: Final[dict[str, Type]] = {}
            for col in model_item.__table__.columns:
                value = model_item.__dict__.get(col.key)
                if value:
                    col_val_mapping[col] = value

            col_name_value_mapping: Final[dict[str, Type]] = {col.key: value for col, value in col_val_mapping.items()}
            if self.engine.name == 'mysql':
                stmt = mysql_insert(model_item.__table__).values(**col_name_value_mapping).prefix_with('IGNORE')
            
            if self.engine.name == 'postgresql':
                stmt = postgres_insert(model_item.__table__).values(**col_name_value_mapping).on_conflict_do_nothing()

            session.execute(stmt)
            session.commit()

            if return_values:
                return session.query(model_item.__table__).filter_by(**col_name_value_mapping).first()