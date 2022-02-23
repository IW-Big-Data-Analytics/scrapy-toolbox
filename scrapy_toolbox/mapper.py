from inspect import getmembers  # Get all classes from a *.py-script
from scrapy import Item
from sqlalchemy.orm.decl_api import DeclarativeMeta
from typing import Dict, Type


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
        self.model_col: Dict[str, DeclarativeMeta] = {
            cls_name + "Item": cls_obj
            for cls_name, cls_obj in getmembers(self.model)
            if isinstance(cls_obj, DeclarativeMeta) and not cls_name == "Base"
        }


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