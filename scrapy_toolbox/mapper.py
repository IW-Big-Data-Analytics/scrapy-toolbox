from inspect import getmembers  # Get all classes from a *.py-script
from scrapy import Item
from sqlalchemy.orm.decl_api import DeclarativeMeta
from typing import Dict


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
        self.model_col: Dict[str, DeclarativeMeta] = {
            cls_name + "Item": cls_obj
            for cls_name, cls_obj in getmembers(self.model)
            if isinstance(cls_obj, DeclarativeMeta) and not cls_name == "Base"
        }

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


    def get_model(self, item): 
        return self.model_col[item.__class__.__name__]
        # primary_keys = [key.name for key in inspect(model_class).primary_key]
        # if not set(primary_keys).issubset(set(list(item.keys()))):
        #     item = model_class(**{i:item[i] for i in item})
        #     return item
        # filter_param = {item_id:item[item_id] for item_id in primary_keys}
        # item_by_id = sess.query(model_class).filter_by(**filter_param).first()
        # if item_by_id is None:
        #     item = model_class(**{i:item[i] for i in item})
        # else:
        #     item = item_by_id
