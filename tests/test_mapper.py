import importlib
from scrapy_toolbox.mapper import ItemsModelMapper
from scrapy_toolbox.exceptions import *
import pytest
from typing import Final, Tuple


@pytest.fixture
def person_items():
    return importlib.import_module('tests.test_resources.items.person_items')

@pytest.fixture
def person_model():
    return importlib.import_module('tests.test_resources.models.person_model')

@pytest.fixture
def person_model_no_hometown():
    return importlib.import_module('tests.test_resources.models.person_model_missing_hometown')

@pytest.fixture
def person_items_no_hometown():
    return importlib.import_module('tests.test_resources.items.person_items_missing_hometown')

@pytest.fixture
def person_items_no_name():
    return importlib.import_module('tests.test_resources.items.person_items_missing_name')

class TestItemsModelMapperInit:
    def test_for_each_model_mapping(self, person_items, person_model):
        """Tests if there is a mapping for each model class.
        """
        mapper: Final[ItemsModelMapper] = ItemsModelMapper(
            items=person_items,
            model=person_model
        )
        assert mapper.model_col.get('NameItem') == person_model.Name
        assert mapper.model_col.get('HometownItem') == person_model.Hometown
        assert mapper.model_col.get('PersonItem') == person_model.Person

    def test_only_model_classes_mapped(self, person_items, person_model):
        '''Tests that in the mapping dictionary only the classes
        specified in the model exist.
        '''
        mapper: Final[ItemsModelMapper] = ItemsModelMapper(
            items=person_items,
            model=person_model
        )

        assert len(mapper.model_col) == 3
        assert mapper.model_col.get('NameItem') == person_model.Name
        assert mapper.model_col.get('HometownItem') == person_model.Hometown
        assert mapper.model_col.get('PersonItem') == person_model.Person


    ## Test Exceptions ##
    def test_no_item_for_model(self, person_items_no_hometown, person_model):
        """
        The ORM Hometown has no corresponding scrapy.Item HometownItem so a
        NoItemForModelException should be raised with text
        'No corresponding scrapy.Item for ORM(s) Hometown.'
        """
        try:
            ItemsModelMapper(
                items=person_items_no_hometown,
                model=person_model
            )
            pytest.fail("Exception not raised.")
        except NoItemForModelException as e:
            expected = 'No corresponding scrapy.Item for ORM(s) Hometown.'
            assert type(e) == NoItemForModelException
            assert str(e) == expected


    def test_item_key_error(self, person_items_no_name, person_model):
        """
        In scrapy.Item PersonItem the Field 'name' for relationship was forgotten. But since in
        ORM Person the Column name exists and in the scrapy.Item the local column name_id is not present
        a KeyMappingException has to be raised
        """
        try:
            ItemsModelMapper(
                items=person_items_no_name,
                model=person_model
            )
            pytest.fail("Exception not raised.")
        except KeyMappingException as e:
            expected = f"""Error for scrapy.Item PersonItem and ORM Person.
                      No Match for Column(s) name, name_id."""
            assert type(e) == KeyMappingException
            assert str(e) == expected

    def test_primary_key_not_null(self, person_items, person_model):
        """
        An exception needs to be raised when a primary key is not set while it does not have a default.
        """
        mapper: Final[ItemsModelMapper] = ItemsModelMapper(
            items=person_items,
            model=person_model
        )
        # id in Hometown is autoincrement => no exception.
        # Field weight is primary_key without default and since name_id and hometown_id are pk the Fields for
        # relationships name and hometown can not be Null.
        # => MissingPrimaryKeyValueException
        item_without_pk = person_items.PersonItem(
            height=1.8,
            shirt_color="red",
        )
        try:
            mapper.map_to_model(item=item_without_pk)
            pytest.fail("Exception was not raised.")
        except MissingPrimaryKeyValueException as e:
            expected = "Primarykey(s) or corresponding Relationship(s) hometown, name, weight are None but do not have default values."
            assert type(e) == MissingPrimaryKeyValueException
            assert str(e) == expected


    def test_relationship_none_or_item(self, person_items, person_model):
        """
        In the Item the Fields that correspond to the relationships are not scrapy.Items so a
        RelationshipItemOrNoneException is raised.
        """
        mapper: Final[ItemsModelMapper] = ItemsModelMapper(
            items=person_items,
            model=person_model
        )
        # Since name and hometown are relationships here they have to be either None or scrapy.Item.
        item = person_items.PersonItem(
            height=1.8,
            shirt_color="red",
            weight=70,
            name="Otto",
            hometown="Köln"
        )
        try:
            mapper.map_to_model(item)
        except RelationshipItemOrNoneException as e:
            expected = "For Relationship(s) hometown, name the values in scrapy.Item need to be scrapy.Item or None."
            assert type(e) == RelationshipItemOrNoneException
            assert str(e) == expected
