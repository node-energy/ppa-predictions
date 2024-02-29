import string

import pytest
import uuid
import pandas as pd
import random
from src.domain import model


def random_malo():
    return ''.join(random.choices(string.digits, k=11))


@pytest.fixture
def customer():
    return model.Customer(ref=uuid.uuid4())


@pytest.fixture
def location(customer):
    return model.Location(ref=uuid.uuid4(), state=model.State.berlin, customer=customer)


@pytest.fixture
def component(location):
    return model.Component(ref=uuid.uuid4(), malo=random_malo(), type='consumer', location=location)


@pytest.fixture
def historic_load_profile(component):
    df = pd.read_csv(
        "tests/unit/historic_load_profile.csv",
        index_col="Timestamp (Europe/Berlin)",
        parse_dates=True,
    )
    return model.HistoricLoadProfile.from_dataframe(
        ref=uuid.uuid4(), component=component, df=df
    )
