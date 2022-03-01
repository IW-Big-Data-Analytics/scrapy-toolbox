import datetime
import os
 
from scrapy import signals
from sqlalchemy.inspection import inspect
from .mapper import ItemsModelMapper
from typing import Final, Type

from sqlalchemy import Table, create_engine, ForeignKey, Column, select
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import URL
from sqlalchemy.orm.exc import NoResultFound
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


class DatabasePipeline(Singleton):
    def __init__(self, settings, items=None, model=None):
        if 'PRODUCTION' in os.environ:
            database_credentials = settings.get('DATABASE')
            self.debug_mode = False
        else:
            database_credentials = settings.get('DATABASE_DEV')
            self.debug_mode = True

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
            self.items = items
            self.model = model
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
        return item
    

    def persist_item(self, item, return_item: bool = False):
        """Controls the persistence of the given item in the database.
        For each given relationship, the given Item is persisted aswell if
        the associated foreign key values are not set already.
        Child items given in a field that has the same name as the relationship in the model
        will be inserted in the database and returned, by calling this method recursivly.
        The now inserted child model-item will replace the given child item.
        Should the associated foreign key values already be set,
        the item from the given relationship will be not inserted into the database.

        Args:
            item (scrapy.Item): 
            return_item (bool): If the model item of the item should be returned.

        Raises:
            AttributeError: If associated foreign key values were not given and also
                not item for the relationship is given. E.g. we have no way to fill
                the forein key values for the model item.

        Returns:
            Type|None: The persisted model item or None.
        """
        model_item = self.mapper.map_to_model(item)
        model_attr: Final[dict] = model_item.__dict__
        foreign_keys: Final[set[ForeignKey]] = model_item.__table__.foreign_keys

        for relationship in inspect(model_item.__class__).relationships: #checking for nested items/relationships
            rel_name: Final[str] = relationship.key
            rel_item = model_attr.get(rel_name)
            associated_fks = [fk for fk in foreign_keys if fk.column.table == relationship.target]

            needed_fk_values_present: Final[bool] = all(model_attr.get(fk.parent.description) is not None for fk in associated_fks)

            if not needed_fk_values_present: #if not all fk values are set
                if not rel_item: 
                        raise AttributeError('Item for relationship missing while associated foreign keys are not set.')
                else:
                    rel_model_item = self.persist_item(rel_item, return_item=True)
                    setattr(model_item, rel_name, rel_model_item)

                    for fk in associated_fks:
                        setattr(model_item, fk.parent.description, getattr(rel_model_item, fk.column.name))
        
        return self.insert_into_db(model_item, return_item)
        
        
    def insert_into_db(self, model_item: Type, return_item: bool=False) -> Type:#|None:
        """Opens a connection to the database and tries to insert the given model item into the database.

        NOTE THAT CONFLICTS WILL BE IGNORED AND ALL COLUMN VALUES NEED TO BE PRESENT

        Foreign key values have to be set beforehand since all conflict are ignored.
        If a model item should be returned the method tries to query an item with
        the given values. If no item could be queried, which is the case if an item that
        was not already stored in the database could not be inserted, an 

        Args:
            model_item (Type): Model item that should be inserted.
            return_item (bool, optional): If the inserted model item should be returned. Defaults to False.

        Returns:
            Type|None: The model item of the inserted model item or None.

        Raises:
            NoResultFound: If no result for the given item was found in the database
                after it should have been inserted.
        """
        with Session(bind=self.engine) as session:
            col_val_mapping: Final[dict[str, Type]] = dict()
            for col in model_item.__table__.columns:
                value = model_item.__dict__.get(col.key)
                if value:
                    if isinstance(value, datetime.datetime):
                        col_val_mapping[col] = value.replace(microsecond=0)
                    else:
                        col_val_mapping[col] = value

            col_name_value_mapping: Final[dict[str, Type]] = {col.key: value for col, value in col_val_mapping.items()}
            if self.engine.name == 'mysql':
                stmt = mysql_insert(model_item.__table__).values(**col_name_value_mapping).prefix_with('IGNORE')
            
            if self.engine.name == 'postgresql':
                stmt = postgres_insert(model_item.__table__).values(**col_name_value_mapping).on_conflict_do_nothing()

            session.execute(stmt)
            session.commit()

            if return_item or self.debug_mode:
                model_item = session.query(model_item.__table__).filter_by(**col_name_value_mapping).first()
                if not model_item:
                    raise NoResultFound(f'No item with values: "{col_name_value_mapping}" persisted.')
                return model_item
            