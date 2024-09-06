import datetime
import random

import factory.alchemy
import pandas as pd
from factory import Sequence, SubFactory, LazyFunction
from faker import Faker
from pandera.typing import DataFrame

from src.domain import model
from src import enums
from src.domain.model import PROGNOSIS_HORIZON_DAYS
from src.enums import PredictionReceiver
from src.utils.dataframe_schemas import TimeSeriesSchema
from src.utils.timezone import TIMEZONE_BERLIN

faker = Faker("de_DE")

TIMESERIES_START = datetime.datetime.combine(datetime.date.today(), datetime.time(0, 0), tzinfo=TIMEZONE_BERLIN) + datetime.timedelta(days=1)
TIMESERIES_END = datetime.datetime.combine(
    (TIMESERIES_START + datetime.timedelta(days=PROGNOSIS_HORIZON_DAYS)).date(), datetime.time(23, 45), tzinfo=TIMEZONE_BERLIN
)
INDEX = pd.date_range(
    start=TIMESERIES_START,
    end=TIMESERIES_END,
    freq="15min",
    name="datetime",
)


class HistoricLoadDataFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = model.HistoricLoadData

    df = DataFrame[TimeSeriesSchema](
        index=INDEX,
        data={"value": [round(random.random() * 100, 3) for _ in range(len(INDEX))]}
    )


class PredictionShipmentFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = model.PredictionShipment

    receiver = PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT


class PredictionFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = model.Prediction

    type = enums.PredictionType.PRODUCTION
    df = DataFrame[TimeSeriesSchema](
        index=INDEX,
        data={"value": [round(random.random() * 100, 3) for _ in range(len(INDEX))]}
    )
    shipments = []


class MarketLocationFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = model.MarketLocation

    number = Sequence(lambda n: f"5{n:010d}")
    measurand = enums.Measurand.POSITIVE
    historic_load_data = SubFactory(HistoricLoadDataFactory)


class LocationSettingsFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = model.LocationSettings

    active_from = datetime.date(2024, 1, 1)
    active_until = None


class ProducerFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = model.Producer

    name = LazyFunction(faker.word)
    market_location = SubFactory(MarketLocationFactory, measurand=enums.Measurand.NEGATIVE)
    prognosis_data_retriever = enums.DataRetriever.ENERCAST_SFTP


class LocationFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = model.Location

    state = enums.State.BERLIN
    alias = LazyFunction(faker.word)
    residual_short = SubFactory(MarketLocationFactory, measurand=enums.Measurand.POSITIVE)
    residual_long = SubFactory(MarketLocationFactory, measurand=enums.Measurand.NEGATIVE)
    producers = factory.List([
        factory.SubFactory(ProducerFactory) for _ in range(1)
    ])
    settings = SubFactory(LocationSettingsFactory)
