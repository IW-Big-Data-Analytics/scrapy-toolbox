import pytest
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError
from scrapy_toolbox.database import DatabasePipeline
import importlib
from tests.test_resources.models.person_model import Person, Name, Hometown

from typing import Final
from types import ModuleType

class TestDatabasePipeline():

    @pytest.mark.usefixtures('setup_database', 'connection')
    def test_autoincrement(setup_database, connection):
        name: Final[Name] = Name(name='Bjarne')
        hometown: Final[Hometown] = Hometown(name='Weyhe', population=15000.0)
        person: Final[Person] = Person(
            weight=80.5,
            height=185.0,
            shirt_color='red',
            name=name,
            hometown=hometown
        )

        settings: Final[dict] = {
            'DATABASE_DEV': {
                'drivername': 'mysql+pymysql',
                'username': 'root',
                'password': 'admin',
                'database': 'test',
                'host': 'localhost',
                'port': '3306'
            }
        }

        person_items: Final[ModuleType] = importlib.import_module('tests.test_resources.items.person_items')
        person_model: Final[ModuleType] = importlib.import_module('tests.test_resources.models.person_model')

        db_pipe: Final[DatabasePipeline] = DatabasePipeline(settings, items=person_items, model=person_model)
        db_pipe.insert_into_db([person])

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


    @pytest.mark.usefixtures('setup_database', 'connection')
    def test_several_inserts(setup_database, connection):
        name_1: Final[Name] = Name(name='Bjarne')
        name_2: Final[Name] = Name(name="Simon")
        hometown: Final[Hometown] = Hometown(name='Weyhe', population=15000.0)
        person_1: Final[Person] = Person(
            weight=80.5,
            height=185.0,
            shirt_color='red',
            name=name_1,
            hometown=hometown
        )
        person_2: Final[Person] = Person(
            weight=76,
            height=182.0,
            shirt_color='white',
            name=name_2,
            hometown=hometown
        )

        settings: Final[dict] = {
            'DATABASE_DEV': {
                'drivername': 'mysql+pymysql',
                'username': 'root',
                'password': 'admin',
                'database': 'test',
                'host': 'localhost',
                'port': '3306'
            }
        }

        person_items: Final[ModuleType] = importlib.import_module('tests.test_resources.items.person_items')
        person_model: Final[ModuleType] = importlib.import_module('tests.test_resources.models.person_model')

        db_pipe: Final[DatabasePipeline] = DatabasePipeline(settings, items=person_items, model=person_model)
        db_pipe.insert_into_db([person_1, person_2])

        with Session(bind=connection) as session:
            stored_persons = session.execute(
                select(Person)
            ).scalars().all()

            stored_person_1 = stored_persons[0]
            stored_person_2 = stored_persons[1]

            assert stored_person_1.height == 185.0
            assert stored_person_2.height == 182.0

            assert (stored_person_1.hometown == stored_person_2.hometown)

            assert stored_person_1.name.id == 1
            assert stored_person_2.name.id == 2



    # TODO: Fails
    @pytest.mark.usefixtures('setup_database', 'connection')
    def test_hometown_inserted_once(setup_database, connection):
        name: Final[Name] = Name(name='Bjarne')
        hometown: Final[Hometown] = Hometown(name='Weyhe', population=15000.0)
        person_1: Final[Person] = Person(
            weight=80.5,
            height=185.0,
            shirt_color='red',
            name=name,
            hometown=hometown
        )
        person_2: Final[Person] = Person(
            weight=80.5,
            height=185.0,
            shirt_color='red',
            name=name,
            hometown=hometown
        )

        settings: Final[dict] = {
            'DATABASE_DEV': {
                'drivername': 'mysql+pymysql',
                'username': 'root',
                'password': 'admin',
                'database': 'test',
                'host': 'localhost',
                'port': '3306'
            }
        }

        person_items: Final[ModuleType] = importlib.import_module('tests.test_resources.items.person_items')
        person_model: Final[ModuleType] = importlib.import_module('tests.test_resources.models.person_model')

        db_pipe: Final[DatabasePipeline] = DatabasePipeline(settings, items=person_items, model=person_model)
        try:
            db_pipe.insert_into_db([person_1, person_2])
        except IntegrityError:
            pytest.fail("Duplicate Insert in Database.")



