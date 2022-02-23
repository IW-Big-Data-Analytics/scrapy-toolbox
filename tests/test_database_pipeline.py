import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session
from scrapy_toolbox.database import DatabasePipeline
import importlib
from tests.test_resources.models.person_model import Person
from tests.test_resources.items.person_items import HometownItem, NameItem, PersonItem
from tests.test_resources.items.person_items_with_fk_fields import PersonItem as PersonItemWithFkFields

from typing import Final
from types import ModuleType

def test_insert_item(connection, db_credentials):
    name: Final[NameItem] = NameItem(name='Bjarne')
    hometown: Final[HometownItem] = HometownItem(name='Weyhe', population=15000.0)
    person: Final[PersonItem] = PersonItem(
        weight=80.5,
        height=185.0,
        shirt_color='red',
        name=name,
        hometown=hometown
    )

    settings: Final[dict] = {
        'DATABASE_DEV': db_credentials
    }

    person_items: Final[ModuleType] = importlib.import_module('tests.test_resources.items.person_items')
    person_model: Final[ModuleType] = importlib.import_module('tests.test_resources.models.person_model')

    db_pipe: Final[DatabasePipeline] = DatabasePipeline(settings, items=person_items, model=person_model)
    db_pipe.persist_item(person)

    with Session(bind=connection) as session:
        stored_person = session.execute(
            select(Person)
        ).first()

        stored_person = stored_person[0]

        assert stored_person.weight == 80.5
        assert stored_person.height == 185.0
        assert stored_person.shirt_color == 'red'

        assert stored_person.name_id == 1
        assert stored_person.name.name == 'Bjarne'

        assert stored_person.hometown_id == 1
        assert stored_person.hometown.name == 'Weyhe'
        assert stored_person.hometown.population == 15000.0


def test_several_inserts(connection, db_credentials):
    name_1: Final[NameItem] = NameItem(name='Bjarne')
    name_2: Final[NameItem] = NameItem(name="Simon")
    hometown: Final[HometownItem] = HometownItem(name='Weyhe', population=15000.0)
    person_1: Final[PersonItem] = PersonItem(
        weight=80.5,
        height=185.0,
        shirt_color='red',
        name=name_1,
        hometown=hometown
    )
    person_2: Final[PersonItem] = PersonItem(
        weight=76,
        height=182.0,
        shirt_color='white',
        name=name_2,
        hometown=hometown
    )

    settings: Final[dict] = {
        'DATABASE_DEV': db_credentials
    }

    person_items: Final[ModuleType] = importlib.import_module('tests.test_resources.items.person_items')
    person_model: Final[ModuleType] = importlib.import_module('tests.test_resources.models.person_model')

    db_pipe: Final[DatabasePipeline] = DatabasePipeline(settings, items=person_items, model=person_model)
    # db_pipe.insert_into_db([person_1, person_2])
    db_pipe.persist_item(person_1)
    db_pipe.persist_item(person_2)

    with Session(bind=connection) as session:
        stored_persons = session.execute(
            select(Person)
        ).scalars().all()

        stored_person_1: Final[Person] = stored_persons[0]
        stored_person_2: Final[Person] = stored_persons[1]

        assert stored_person_1.weight == 80.5
        assert stored_person_1.height == 185.0
        assert stored_person_1.shirt_color == 'red'
        assert stored_person_1.name.id == 1
        assert stored_person_1.name_id == 1
        assert stored_person_1.hometown.id == 1
        assert stored_person_1.hometown_id == 1

        assert stored_person_2.weight == 76
        assert stored_person_2.height == 182.0
        assert stored_person_2.shirt_color == 'white'
        assert stored_person_2.name.id == 2
        assert stored_person_2.name_id == 2
        assert stored_person_2.hometown.id == 1
        assert stored_person_2.hometown_id == 1

        assert stored_person_1.hometown == stored_person_2.hometown
        assert stored_person_1.name != stored_person_2.name


def test_duplicate_insert(connection, db_credentials):
    name: Final[NameItem] = NameItem(name='Bjarne')
    hometown: Final[HometownItem] = HometownItem(name='Weyhe', population=15000.0)
    person_1: Final[PersonItem] = PersonItem(
        weight=80.5,
        height=185.0,
        shirt_color='red',
        name=name,
        hometown=hometown
    )

    settings: Final[dict] = {
        'DATABASE_DEV': db_credentials
    }

    person_items: Final[ModuleType] = importlib.import_module('tests.test_resources.items.person_items')
    person_model: Final[ModuleType] = importlib.import_module('tests.test_resources.models.person_model')

    db_pipe: Final[DatabasePipeline] = DatabasePipeline(settings, items=person_items, model=person_model)
    db_pipe.persist_item(person_1)
    db_pipe.persist_item(person_1)
    db_pipe.persist_item(person_1)

    with Session(bind=connection) as session:
        stored_persons = session.execute(
            select(Person)
        ).scalars().all()
        assert len(stored_persons) == 1

        stored_person_1 = stored_persons[0]
        assert stored_person_1.weight == 80.5
        assert stored_person_1.height == 185.0
        assert stored_person_1.shirt_color == 'red'
        assert stored_person_1.name.id == 1
        assert stored_person_1.name_id == 1
        assert stored_person_1.hometown.id == 1
        assert stored_person_1.hometown_id == 1


def test_child_already_inserted(connection, db_credentials):
    name: Final[NameItem] = NameItem(name='Bjarne')
    hometown: Final[HometownItem] = HometownItem(name='Weyhe', population=15000.0)
    person_1: Final[PersonItem] = PersonItem(
        weight=80.5,
        height=185.0,
        shirt_color='red',
        name=name,
        hometown=hometown
    )

    settings: Final[dict] = {
        'DATABASE_DEV': db_credentials
    }

    person_items: Final[ModuleType] = importlib.import_module('tests.test_resources.items.person_items')
    person_model: Final[ModuleType] = importlib.import_module('tests.test_resources.models.person_model')

    db_pipe: Final[DatabasePipeline] = DatabasePipeline(settings, items=person_items, model=person_model)
    db_pipe.persist_item(name)
    db_pipe.persist_item(hometown)
    db_pipe.persist_item(person_1)

    with Session(bind=connection) as session:
        stored_persons = session.execute(
            select(Person)
        ).scalars().all()
        assert len(stored_persons) == 1

        stored_person_1 = stored_persons[0]
        assert stored_person_1.weight == 80.5
        assert stored_person_1.height == 185.0
        assert stored_person_1.shirt_color == 'red'
        assert stored_person_1.name.id == 1
        assert stored_person_1.name_id == 1
        assert stored_person_1.hometown.id == 1
        assert stored_person_1.hometown_id == 1


def test_foreign_keys_given(connection, db_credentials):
    name: Final[NameItem] = NameItem(name='Bjarne')
    hometown: Final[HometownItem] = HometownItem(name='Weyhe', population=15000.0)
    person_1: Final[PersonItemWithFkFields] = PersonItemWithFkFields(
        weight=80.5,
        height=185.0,
        shirt_color='red',
        name_id=1, #we only know because of empty database
        hometown_id=1 #we only know because of empty database
    )

    settings: Final[dict] = {
        'DATABASE_DEV': db_credentials
    }

    person_items: Final[ModuleType] = importlib.import_module('tests.test_resources.items.person_items_with_fk_fields')
    person_model: Final[ModuleType] = importlib.import_module('tests.test_resources.models.person_model')

    db_pipe: Final[DatabasePipeline] = DatabasePipeline(settings, items=person_items, model=person_model)
    db_pipe.persist_item(name)
    db_pipe.persist_item(hometown)
    db_pipe.persist_item(person_1)

    with Session(bind=connection) as session:
        stored_persons = session.execute(
            select(Person)
        ).scalars().all()
        assert len(stored_persons) == 1

        stored_person_1 = stored_persons[0]
        assert stored_person_1.weight == 80.5
        assert stored_person_1.height == 185.0
        assert stored_person_1.shirt_color == 'red'
        assert stored_person_1.name.id == 1
        assert stored_person_1.name_id == 1
        assert stored_person_1.hometown.id == 1
        assert stored_person_1.hometown_id == 1