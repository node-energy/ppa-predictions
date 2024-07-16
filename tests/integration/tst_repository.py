import pytest
import datetime as dt
import pandas as pd
import uuid
from src.persistence.repository import LocationRepository
from src.persistence.sqlalchemy import Location as DBLocation
from src.domain.model import Consumer, HistoricLoadData, Location, Prediction, PredictionType, Producer, State


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
        session = sqlite_session_factory()
        repo = LocationRepository(session=session, db_cls=DBLocation)
        residual_short = Consumer(malo="malo-1")
        residual_short.historic_load_data = HistoricLoadData(df=create_df_with_constant_values())
        residual_long = Producer(malo="malo-2")
        producer_1 = Producer(malo="malo-3")
        prediction_short = Prediction(type=PredictionType.RESIDUAL_SHORT, df=create_df_with_constant_values())
        prediction_consumption = Prediction(type=PredictionType.CONSUMPTION, df=create_df_with_constant_values())
        location = Location(
            id=uuid.UUID("64c4a7dd-242e-48a3-8932-3f85f1d6009b"),
            state=State.berlin,
            residual_short=residual_short,
            residual_long=residual_long,
            producers=[producer_1],
            predictions=[prediction_short, prediction_consumption]
        )
        repo.add(location)
        #session.commit()  # Can be removed
        assert repo.get(uuid.UUID("64c4a7dd-242e-48a3-8932-3f85f1d6009b")) == location
