from inspect import getmembers, isclass  # Get all classes from a *.py-script
from sqlalchemy.inspection import inspect  # Get PKs from model-class
from scrapy import Item
from sqlalchemy.orm.decl_api import DeclarativeMeta


class ItemsModelMapper:
    # For each Item there has to be a corrisponding databaseobject that extends scrapy_toolbox.database.DeclarativeBase
    # The naming must be XYItem for Item and XY for databaseobject
    # The item must have a ids variable with all the names of primary-keys to filter or empty list

    def __init__(self, items, model):
        self.items = items
        self.model = model
        self.model_col = {
            cls_name + "Item": cls_obj for cls_name, cls_obj in
            getmembers(self.model) if isinstance(cls_obj, DeclarativeMeta) and not cls_name == "Base"
        }  # "XYItem": XY.__class_

    def map_to_model(self, item: Item, sess) -> DeclarativeMeta:
        for key in item:
            if isinstance(item[key], Item):
                item[key] = self.map_to_model(item[key], sess)
        model_class: DeclarativeMeta = self.model_col[item.__class__.__name__]  # get model for item name
        primary_keys = [key.name for key in inspect(model_class).primary_key]
        if not set(primary_keys).issubset(set(list(item.keys()))):
            item = model_class(**{i: item[i] for i in item})
            return item
        filter_param = {item_id: item[item_id] for item_id in primary_keys}
        item_by_id = sess.query(model_class).filter_by(**filter_param).first()
        if item_by_id is None:
            item = model_class(**{i: item[i] for i in item})
        else:
            item = item_by_id
        return item
