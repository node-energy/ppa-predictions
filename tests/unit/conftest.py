import string

import pytest
import uuid
import pandas as pd
import random
from src.domain import model


def random_malo():
    return ''.join(random.choices(string.digits, k=11))


@pytest.fixture
def company():
    return model.Company(id=uuid.uuid4(), name="corp")


@pytest.fixture
def location(company):
    return model.Location(id=uuid.uuid4(), state=model.State.berlin, company=company)


@pytest.fixture
def component(location):
    return model.Component(id=uuid.uuid4(), malo=random_malo(), type='consumer', location=location)


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
