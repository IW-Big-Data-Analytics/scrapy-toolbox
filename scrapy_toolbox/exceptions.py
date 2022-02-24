class MapperException(Exception):
    """
    Basic Template for constructor for all exceptions.
    Get names of objects that caused the error and put them in string for
    error message in __str__ method.
    """
    def __init__(self, diff: set):
        self.diff_str = ", ".join(diff)
        super().__init__()


class NoModelForItemException(MapperException):
    """
    Raises if there is a scrapy.Item that does not have a
    corresponding model object.
    """
    def __str__(self):
        message = f"No corresponding ORM objects for scrapy.Item(s) {self.diff_str}."
        return message


class NoItemForModelException(MapperException):
    """
    Raises if there is a model object that does not have a
    corresponding scrapy.Item.
    """
    def __str__(self):
        message = f"No corresponding scrapy.Item for ORM(s) {self.diff_str}."
        return message


class KeyMappingException(MapperException):
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
        self.item_name = item_name
        super().__init__(diff)

    def __str__(self):
        message = f"""Error for scrapy.Item {self.item_name} and ORM {self.item_name.replace("Item", "")}.
                      No Match for Column(s) {self.diff_str}."""
        return message


class MissingPrimaryKeyValueException(MapperException):
    """
    Raises if a model objects is created which has some primary_keys missing that do not have a default value.
    """
    def __str__(self):
        message = f"Primary keys {self.diff_str} are None but do not have default values."
        return message


class NoRelationshipException(MapperException):
    """
    Raises if a scrapy.Item contains another scrapy.Item but in the model there is no relationship defined
    for those Items.
    Example:
        class Person(Base):
            name = Column(String(255), primary_key=True)

        class Account(Base):
            id = Column(Integer, primary_key=True, autoincrement=True)
            person_name = Column(String(255), ForeignKey("persons.name"))
            person = relationship("Person")

        Only then the Items AccountItem(person = PersonItem(...)) successfully passes the pipeline.
    """
    def __str__(self):
        message = f"If model object has relationship the corresponding column has to contain a scrapy.Item or None." \
                  f"Was not the case for {self.diff_str}."
        return message
