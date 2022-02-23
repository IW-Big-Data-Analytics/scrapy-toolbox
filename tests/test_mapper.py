import importlib
from scrapy_toolbox.mapper import ItemsModelMapper
from scrapy_toolbox.exceptions import NoItemForModelException
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
        NoModelForItemException shoult be raised with text
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

