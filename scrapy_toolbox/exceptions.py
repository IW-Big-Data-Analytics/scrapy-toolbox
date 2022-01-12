class NoModelForItemException(Exception):
    """
    Raises if there is a scrapy.Item that does not have a
    corresponding model object.
    """
    def __init__(self, diff: set):
        self.diff = diff
        super().__init__()

    def __str__(self):
        diff_str = ", ".join(self.diff)
        message = f"No corresponding model objects for items {diff_str}. Please create these Items"
        return message


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
    def __init__(self, diff, item_name):
        self.diff = diff
        self.item_name = item_name
        super().__init__()

    def __str__(self):
        keys = ", ".join(self.diff)
        message = f"""Error for item {self.item_name} and model {self.item_name.replace("Item", "")}. \n 
                Keys {keys} have no match in model columns.\n
                Please check for typos or add these colums."""
        return message




class MissingPrimaryKeyValueException(Exception):
    """
    Raises if a model objects is created which has some primary_keys missing that do not have a default value.
    """
    pass


class NoRelationshipException(Exception):
    """
    Raises if a scrapy.Items contains another scrapy.Items but in the model there is no relationship defined
    for those Items.
    Example:
        class Person(Base):
            name = Column(String(255), primary_key=True)

        class Account(Base):
            id = Column(Integer, primary_key=True, autoincrement=True)
            person_name = Column(String(255), ForeignKey("persons.name"))
            person = relationship("Person")

        Only then the Items AccountItem(person = PersonItem(...)) sucessfully passes the pipeline.
    """
    pass
