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
from src.enums import TransmissionSystemOperator
from src.utils.market_location_number_validator import MarketLocationNumberGenerator


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
def location():
    return model.Location(
        settings=model.LocationSettings(
            active_from=dt.date(2024, 1, 1),
            active_until=None,
            send_consumption_predictions_to_fahrplanmanagement=True,
            historic_days_for_consumption_prediction=50,
        ),
        state=enums.State.BERLIN,
        residual_short=model.MarketLocation(number=MarketLocationNumberGenerator()(), measurand=enums.Measurand.POSITIVE),
        tso=TransmissionSystemOperator.AMPRION,
    )


@pytest.fixture
def producer():
    return model.Producer(market_location=MarketLocation(number=MarketLocationNumberGenerator()(), measurand=enums.Measurand.NEGATIVE), prognosis_data_retriever=enums.DataRetriever.ENERCAST_SFTP)