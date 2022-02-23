from inspect import getmembers, isclass  # Get all classes from a *.py-script
import scrapy
from sqlalchemy.inspection import inspect
from scrapy import Item
from sqlalchemy.orm.decl_api import DeclarativeMeta
from .exceptions import *
from typing import Dict, Type, Tuple


class ItemsModelMapper:
    def __init__(self, items, model):
        """
        Classes in module "model" have to extend DeclarativeMeta.
        Models have to be defined like:

        Base = declarative_base()
        class Bla(Base):
            ...

        Args:
            items: module items.py from current project with scrapy.Items.
            model: module models.py from current project with SQLAlchemy objects.
        """
        self.items = items
        self.model = model
        self.item_classes = getmembers(self.items, lambda x: isclass(x) and issubclass(x, scrapy.Item))
        self.model_col: Dict[str, DeclarativeMeta] = {
            cls_name + "Item": cls_obj
            for cls_name, cls_obj in getmembers(self.model, isclass)
            if isinstance(cls_obj, DeclarativeMeta) and not cls_name == "Base"
        }
        # Do checks
        item_error, diff = self._check_item_mapping(
            item_names=set(dict(self.item_classes).keys()),
            mapper_item_names=set(self.model_col.keys())
        )
        if item_error:
            raise NoItemForModelException(diff)

        # for item_name, item_class in self.item_classes:
        #     key_error, diff = self._check_keys(
        #         model_class=self.model_col[item_name],
        #         item_class=item_class
        #     )
        #     if key_error:
        #         raise KeyMappingException(diff=diff, item_name=item_name)


    def _check_relationships_are_items_or_none(self, relationships: set, item: Item) -> Tuple[bool, set]:
        """
        Check wether all relationships from model are either None or scrapy.Item.
        :param relationships: set with all relationships from model_class.
        :param item: item that is getting processed
        :return: bool that is True when a relationship is not set correctly, all keys that are not set correctly
        """
        relationships_not_null = relationships.intersection(set(item.keys()))
        relationship_error = False
        for relationship in relationships_not_null.copy():
            if isinstance(item[relationship], Item):
                relationships_not_null.remove(relationship)
            else:
                relationship_error = True
        return relationship_error, relationships_not_null


    def _check_primary_keys_not_null(self, item: Item, model_class: DeclarativeMeta) -> Tuple[bool, set]:
        """
        Check if all primary keys without default value or auto_increment have values.
        :param item: scrapy.Item that is processed
        :param model_class: class of the corresponding model.
        :return: True if there is a pk that is null, set with pks that are null.
        """
        relationships = {rel for rel in inspect(model_class).relationships}
        primary_keys = {key.name for key in inspect(model_class).primary_key
                        if key.default is None and key.autoincrement is not True}
        arguments_none = set(item.fields.keys()).difference(set(item.keys()))
        for relationship in relationships:
            relationship_key = relationship.key
            # Get corresponding columns for the relationship that are PKs.
            locale_columns_pk = {col.name for col in relationship.local_columns
                                 if col.primary_key is True}
            # If the relationship column has a value remove all corresponding pks
            # from set with arguments from item that are none.
            if relationship_key not in arguments_none:
                primary_keys = primary_keys.difference(locale_columns_pk)
                arguments_none = arguments_none.difference(locale_columns_pk)
            else:
                primary_key_error = not locale_columns_pk.issubset(arguments_none)
                diff = {relationship_key}
                if primary_key_error:
                    return True, diff
        primary_key_error = not primary_keys.issubset(arguments_none)
        diff = primary_keys.intersection(arguments_none)
        return primary_key_error, diff

    def _check_item_mapping(self, item_names: set, mapper_item_names: set) -> Tuple[bool, set]:
        """
        Check if for each model class there exists a corresponding item.
        :param item_names: Names from the Item classes for current project
        :param mapper_item_names: Names from the dictionary that got created in __init__
        :return: bool if all models have a item, all models that dont have a item
        """
        mapping_error = not mapper_item_names.issubset(item_names)
        difference = mapper_item_names.difference(item_names)
        diff = {self.model_col[item_name].__name__ for item_name in difference}
        return mapping_error, diff


    def _check_keys(self, model_class: DeclarativeMeta, item_class: scrapy.Item) -> Tuple[bool, set]:
        """
        check if for each item the corresponding model has the same keys.
        :param model_class: corresponding model class
        :param item_class: item class
        :return: bool if keys are all right, all keys that do not have a match
        """
        item_fields = set(item_class.fields.keys())
        model_fields = set(inspect(model_class).columns.keys())
        model_relationships = set(inspect(model_class).relationships.keys())
        model_columns = model_fields.union(model_relationships)
        key_error = not item_fields.issubset(model_columns)
        diff = item_fields.difference(model_fields)
        return key_error, diff

    def map_to_model(self, item: Item, map_children: bool = False) -> Type:
        """Get scrapy.Item from DatabasePipeline.process_item function and return the corresponding
        model from module model.

        Args:
            item (Item): The scrapy.Item from DatabasePipeline.process_item.
            map_children (bool): If items that are passed as a field value of the actual
                given item should be mapped aswell. Defaults to False.

        Returns
            Type: corresponding model object.
        """
        if map_children:
            for key in item:
                if isinstance(item[key], Item):
                    item[key] = self.map_to_model(item[key])
        model_class: DeclarativeMeta = self.model_col[item.__class__.__name__]  # get model for item name
        model_object: model_class = model_class(**{i: item[i] for i in item})
        return model_object
