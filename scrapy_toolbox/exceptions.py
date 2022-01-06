class NoModelForItemException(Exception):
    """
    Raises if there is a scrapy.Item that does not have a
    corresponding model object.
    """
    pass

class NoItemForModelException(Exception):
    """
    Raises if there is a model object that does not have a
    corresponding scrapy.Item.
    """
    pass

class KeyMappingException(Exception):
    """
    Raises if there exists a scrapy.Item with corresponding model object
    where the keys do not match.
    Example:
        class AbcItem(scrapy.Item):
            id = scrapy.Field()
        class Abc(Base):
            ids = Column(String(255))
    Keys from AbcItem and Abc do not match.
    """
    pass

class MissingPrimaryKeyValueException(Exception):
    """
    Raises if a model objects is created which has some primary_keys missing that do not have a default value.
    """
    pass