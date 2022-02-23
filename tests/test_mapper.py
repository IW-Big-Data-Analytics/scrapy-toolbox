import importlib
from scrapy_toolbox.mapper import ItemsModelMapper
from scrapy_toolbox.exceptions import NoModelForItemException
import pytest
from typing import Final


@pytest.fixture
def person_items():
    return importlib.import_module('tests.test_resources.items.person_items')

@pytest.fixture
def person_model():
    return importlib.import_module('tests.test_resources.models.person_model')

@pytest.fixture
def get_person_items_missing_hometown():
    return person_items_missing_hometown


class TestItemsModelMapperInit:
    def test_for_each_model_mapping(self):
        """Tests if there is a mapping for each model class.
        """
        mapper: Final[ItemsModelMapper] = ItemsModelMapper(
            items=person_items,
            model=person_model
        )
        assert mapper.model_col.get('NameItem') == person_model.Name
        assert mapper.model_col.get('HometownItem') == person_model.Hometown
        assert mapper.model_col.get('PersonItem') == person_model.Person

    def test_only_model_classes_mapped(self):
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

    def test_missing_item_for_model(self):
        """Checking if a KeyMappingException is thrown when there is 
        an item missing for a model.

        Args:
            person_model (ModuleType): Module containing the database model.
            person_items_missing_hometown (ModuleType): Module containing the items but missing
                a HometownItem.
        """
        with pytest.raises(NoItemForModelException):
            mapper: Final[ItemsModelMapper] = ItemsModelMapper(
                items=None,
                model=person_model
            )
