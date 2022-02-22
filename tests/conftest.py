import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy_utils import database_exists, create_database
from typing import Final
import yaml
from pathlib import Path
import tests.test_resources.models.person_model as models


@pytest.fixture(scope='session')
def db_credentials() -> dict[str, str]:
    """Returns the database credentials which should reside in a
    untracked secrets.yaml file.
    Returns:
        dict[str, str]: 
            Structure is the following:
                {
                    drivername: ""
                    username: ""
                    password: ""
                    database: ""
                    host: ""
                    port: ""
                }
    """
    secrets_path: Final[Path] = Path('.', 'tests','secrets.yaml')
    with open(secrets_path, 'r') as stream:
        try:
            return yaml.safe_load(stream)['test_database_credentials']
        except yaml.YAMLError as exc:
            print(exc)


@pytest.fixture(scope="session")
@pytest.mark.usefixtures('db_credentials')
def connection(db_credentials):
    """Creates the database based on the given credentials and
    mapping infos if necessary and establishes a connection to it.
    Returns the engine.
    Args:
        db_credentials (dict[str, str]): DB-credentials -> see fixture.
    Returns:
        Engine: The created database engine.
    """
    engine: Final[Engine] = create_engine(
        '{drivername}://{username}:{password}@{host}:{port}/{database}'.format(
            **db_credentials
        )
    )
    if not database_exists(engine.url):
        create_database(engine.url)

    return engine


@pytest.mark.usefixtures()
def seed_database(engine: Engine):
    """Fills the the database with content.
    The engine is expected to have a connection that was made to the database that was created from
    the test credentials and holds the tables specified in the models.py.
    Args:
        engine (Engine): Database engine from the test database
    """
    pass

@pytest.fixture(scope="session")
@pytest.mark.usefixtures('connection') #add potential content fixtures here
def setup_database(connection: Engine):
    """Sets up the database and yields the state.
    After the tests are finished, all content from the database is erased.

    This method yields an empty database. Use setup_database_with_seed to 
    yield a database filled with default content.

    Args:
        connection (Engine): Connection to the database.

    Yields:
        None: The state of the database.
    """
    models.Base.metadata.bind = connection
    models.Base.metadata.create_all()
    
    yield #yield filled db

    models.Base.metadata.drop_all() #delete everything


@pytest.fixture(scope="session")
@pytest.mark.usefixtures('connection')
def setup_database_with_seed(connection: Engine):
    """Sets up the database connection.
    Fills the database with the given content and yields the state of the database.
    Afterwards all the content in the database is deleted.
    Args:
        connection (Engine): Connection to the database.
        user_table_content (list[dict]): Content for the 'users' table.
        house_table_content (list[dict]): Content for the 'houses' table.
    """
    models.Base.metadata.bind = connection
    models.Base.metadata.create_all()
    
    seed_database(connection)
    
    yield #yield filled db

    models.Base.metadata.drop_all() #delete everything