import datetime as dt
import uuid

import pandas as pd

from src.domain.model import HistoricLoadData, Location, Prediction, Producer, LocationSettings, \
    MarketLocation
from src.enums import PredictionType, State, Measurand, DataRetriever
from src.persistence.repository import LocationRepository
from src.persistence.sqlalchemy import Location as DBLocation


def create_df_with_constant_values(value=42):
    start = dt.datetime.now().replace(microsecond=0, second=0, minute=0)
    end = start + dt.timedelta(days=30)
    df = pd.DataFrame(
        {"datetime": pd.date_range(start=start, end=end, freq="15min"), "value": value}
    )
    df.set_index("datetime", inplace=True)
    return df


class TestLocationRepository:
    def test_get_location_by_id(self, sqlite_session_factory):
        # TODO test does not work anymore with sqlite database because model.Prediction has an ARRAY field now which is not supported by sqlite
        session = sqlite_session_factory()
        repo = LocationRepository(session=session, db_cls=DBLocation)
        settings = LocationSettings(
            active_from=dt.date(2024, 1, 1),
            active_until=dt.date(2025, 1, 1),
        )
        residual_short = MarketLocation(number="market_location-1", measurand=Measurand.POSITIVE)
        residual_short.historic_load_data = HistoricLoadData(df=create_df_with_constant_values())
        residual_long = MarketLocation(number="market_location-2", measurand=Measurand.NEGATIVE)
        producer_1 = Producer(market_location=MarketLocation(number="market_location-3", measurand=Measurand.POSITIVE), prognosis_data_retriever=DataRetriever.ENERCAST_SFTP)
        prediction_short = Prediction(type=PredictionType.RESIDUAL_SHORT, df=create_df_with_constant_values())
        prediction_consumption = Prediction(type=PredictionType.CONSUMPTION, df=create_df_with_constant_values())
        location = Location(
            id=uuid.UUID("64c4a7dd-242e-48a3-8932-3f85f1d6009b"),
            settings=settings,
            state=State.BERLIN,
            residual_short=residual_short,
            residual_long=residual_long,
            producers=[producer_1],
            predictions=[prediction_short, prediction_consumption]
        )
        repo.add(location)
        assert repo.get(uuid.UUID("64c4a7dd-242e-48a3-8932-3f85f1d6009b")) == location
