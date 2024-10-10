import datetime as dt
import uuid

import pandas as pd

from src.prognosis.persistence import LocationRepository
from src.prognosis.persistence.sqlalchemy import Location as DBLocation
from tests.test_prognosis.factories import LocationFactory


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
        location = LocationFactory.build(
            id=uuid.UUID("64c4a7dd-242e-48a3-8932-3f85f1d6009b")
        )

        repo.add(location)
        assert repo.get(uuid.UUID("64c4a7dd-242e-48a3-8932-3f85f1d6009b")) == location
