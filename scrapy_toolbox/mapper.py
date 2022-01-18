from inspect import getmembers, isclass  # Get all classes from a *.py-script
import scrapy
from sqlalchemy.inspection import inspect
from scrapy import Item
from sqlalchemy.orm.decl_api import DeclarativeMeta
from typing import Dict, Tuple
from exceptions import NoItemForModelException, KeyMappingException


class ItemsModelMapper:

    def __init__(self, items, model):
        """
        Classes in module "model" have to extend DeclarativeMeta.
        Models have to be defined like:

        Base = declarative_base()
        class Bla(Base):
            ...

        :param items: module items.py from current project with scrapy.Items.
        :param model: module models.py from current project with SQLAlchemy objects.
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
            item_names=dict(self.item_classes).keys(),
            mapper_item_names=self.model_col.keys()
        )
        if item_error:
            raise NoItemForModelException(diff)

        for item_name, item_class in self.item_classes:
            key_error, diff = self._check_keys(
                model_class=self.model_col[item_name],
                item_class=item_class
            )
            if key_error:
                raise KeyMappingException(diff=diff, item_name=item_name)


    def map_to_model(self, item: Item):
        """
        Get scrapy.Item from DatabasePipeline.process_item function and return the corresponding
        model from module model.
        :param item: The scrapy.Item from DatabasePipeline.process_item.
        :return: corresponding model object.
        """
        for key in item:
            if isinstance(item[key], Item):
                item[key] = self.map_to_model(item[key])
        model_class: DeclarativeMeta = self.model_col[item.__class__.__name__]  # get model for item name
        model_object: model_class = model_class(**{i: item[i] for i in item})
        return model_object

    def _check_item_mapping(self, item_names, mapper_item_names) -> Tuple[bool, set]:
        """
        Check if for each model class there exists a corresponding item.
        :param item_names: Names from the Item classes for current project
        :param mapper_item_names: Names from the dictionary that got created in __init__
        :return: bool if all models have a item, all models that dont have a item
        """
        mapping_error = not set(mapper_item_names).issubset(set(item_names))
        difference = set(mapper_item_names).difference(set(item_names))
        return mapping_error, difference

    def _check_keys(self, model_class, item_class) -> Tuple[bool, set]:
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


