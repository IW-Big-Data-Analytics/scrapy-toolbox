from scrapy_toolbox.mapper import ItemsModelMapper
from tests.test_resources.items.person_items import person_items, person_items_missing_hometown
from tests.test_resources.models import person_model
from tests.test_resources.models.person_model import Person, Name, Hometown
from scrapy_toolbox.exceptions import KeyMappingException
import pytest
from typing import Final

@pytest.fixture
def person_items():
    return person_items

@pytest.fixture
def person_model():
    return person_model

@pytest.fixture
def person_items_missing_hometown():
    return person_items_missing_hometown

class TestItemsModelMapperInit:
    def test_for_each_model_mapping(self, person_items, person_model):
        """Tests if there is a mapping for each model class.
        """
        mapper: Final[ItemsModelMapper] = ItemsModelMapper(
            items=person_items,
            model=person_model
        )

        assert mapper.model_col.get('NameItem') == Name
        assert mapper.model_col.get('HometownItem') == Hometown
        assert mapper.model_col.get('PersonItem') == Person

    def test_only_model_classes_mapped(self, person_items, person_model):
        '''Tests that in the mapping dictionary only the classes
        specified in the model exist.
        '''
        mapper: Final[ItemsModelMapper] = ItemsModelMapper(
            items=person_items,
            model=person_model
        )

        assert len(mapper.model_col) == 3
        assert mapper.model_col.get('NameItem') == Name
        assert mapper.model_col.get('HometownItem') == Hometown
        assert mapper.model_col.get('PersonItem') == Person


    def test_missing_item_for_model(self, person_model, person_items_missing_hometown):
        """Checking if a KeyMappingException is thrown when there is 
        an item missing for a model.

        Args:
            person_model (ModuleType): Module containing the database model.
            person_items_missing_hometown (ModuleType): Module containing the items but missing
                a HometownItem.
        """
        with pytest.raises(KeyMappingException):
            mapper: Final[ItemsModelMapper] = ItemsModelMapper(
                items=person_items_missing_hometown,
                model=person_model
            )