import datetime
import random

import factory.alchemy
import pandas as pd
from factory import Sequence, SubFactory, LazyFunction
from faker import Faker
from pandera.typing import DataFrame

from src.domain import model
from src import enums
from src.enums import PredictionReceiver
from src.utils.dataframe_schemas import TimeSeriesSchema
from src.utils.timezone import TIMEZONE_BERLIN

faker = Faker("de_DE")

PROGNOSIS_HORIZON_DAYS = 7


def _generate_prediction_df():
    start = datetime.datetime.combine(
        datetime.date.today(),
        datetime.time(0, 0),
        tzinfo=TIMEZONE_BERLIN
    ) + datetime.timedelta(days=1)
    end = datetime.datetime.combine(
        start,
        datetime.time(23, 45),
        tzinfo=TIMEZONE_BERLIN
    ) + datetime.timedelta(days=PROGNOSIS_HORIZON_DAYS)

    index = pd.date_range(
        start=start,
        end=end,
        freq="15min",
        name="datetime",
    )
    return DataFrame[TimeSeriesSchema](
        index=index,
        data={"value": [round(random.random() * 100, 3) for _ in range(len(index))]}
    )


def _generate_historic_df():
    start = datetime.datetime.combine(
        datetime.date.today(),
        datetime.time(0, 0),
        tzinfo=TIMEZONE_BERLIN
    ) - datetime.timedelta(days=51)
    end = datetime.datetime.combine(
        datetime.date.today(),
        datetime.time(23, 45),
        tzinfo=TIMEZONE_BERLIN
    ) - datetime.timedelta(days=1)

    index = pd.date_range(
        start=start,
        end=end,
        freq="15min",
        name="datetime",
    )
    return DataFrame[TimeSeriesSchema](
        index=index,
        data={"value": [round(random.random() * 100, 3) for _ in range(len(index))]}
    )


class HistoricLoadDataFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = model.HistoricLoadData

    df = _generate_historic_df()


class PredictionShipmentFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = model.PredictionShipment

    receiver = PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT


class PredictionFactory(factory.alchemy.SQLAlchemyModelFactory):
    class Meta:
        model = model.Prediction

    type = enums.PredictionType.PRODUCTION
    df = _generate_prediction_df()
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
    send_consumption_predictions_to_fahrplanmanagement = True
    historic_days_for_consumption_prediction = 50


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
    tso = enums.TransmissionSystemOperator.AMPRION
    residual_short = SubFactory(MarketLocationFactory, measurand=enums.Measurand.POSITIVE)
    residual_long = SubFactory(MarketLocationFactory, measurand=enums.Measurand.NEGATIVE)
    producers = factory.List([
        factory.SubFactory(ProducerFactory) for _ in range(1)
    ])
    settings = SubFactory(LocationSettingsFactory)
