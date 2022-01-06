from inspect import getmembers  # Get all classes from a *.py-script
from scrapy import Item
from sqlalchemy.orm.decl_api import DeclarativeMeta


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
        self.model_col = {
            cls_name + "Item": cls_obj
            for cls_name, cls_obj in getmembers(self.model)
            if isinstance(cls_obj, DeclarativeMeta) and not cls_name == "Base"
        }

    def map_to_model(self, item: Item) -> DeclarativeMeta:
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
        model_object = model_class(**{i: item[i] for i in item})
        return model_object

