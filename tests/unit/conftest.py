import datetime as dt
import string

import pytest
import uuid
import pandas as pd
import random
from sqlalchemy import create_engine, MetaData
from sqlalchemy.orm import sessionmaker

from src import enums
from src.domain import model
from src.domain.model import MarketLocation


@pytest.fixture
def in_memory_sqlite_db():
    engine = create_engine("sqlite:///:memory:")
    metadata = MetaData()
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
    return model.Location(
        settings=model.LocationSettings(
            active_from=dt.date(2024, 1, 1),
            active_until=None,
        ),
        state=enums.State.berlin,
        residual_short=model.MarketLocation(number=random_malo(), measurand=enums.Measurand.POSITIVE)
    )


@pytest.fixture
def producer():
    return model.Producer(market_location=MarketLocation(number=random_malo(), measurand=enums.Measurand.NEGATIVE), prognosis_data_retriever=enums.DataRetriever.ENERCAST_SFTP)