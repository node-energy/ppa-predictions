import string

import pytest
import uuid
import pandas as pd
import random
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker, clear_mappers
from src.domain import model
from src.persistence.sqlalchemy import Base


@pytest.fixture
def in_memory_sqlite_db():
    engine = create_engine("sqlite:///db1.db") #create_engine("sqlite:///:memory:")
    #metadata = MetaData()
    metadata = Base.metadata
    metadata.create_all(engine)
    return engine


@pytest.fixture
def sqlite_session_factory(in_memory_sqlite_db):
    yield sessionmaker(bind=in_memory_sqlite_db)


def random_malo():
    return ''.join(random.choices(string.digits, k=11))


@pytest.fixture
def historic_load_profile(component):
    df = pd.read_csv(
        "tests/unit/historic_load_profile.csv",
        index_col="Timestamp (Europe/Berlin)",
        parse_dates=True,
    )
    return model.HistoricLoadProfile.from_dataframe(
        id=uuid.uuid4(), component=component, df=df
    )


@pytest.fixture
def location():
    return model.Location(state=model.State.berlin, residual_short=model.Consumer(malo=random_malo()))


@pytest.fixture
def producer():
    return model.Producer(malo=random_malo())