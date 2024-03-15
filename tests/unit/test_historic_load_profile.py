import uuid
import pandas as pd
from src.domain import model


def test_create_historic_load_profile(component):
    df = pd.read_csv('tests/unit/historic_load_profile.csv', index_col='Timestamp (Europe/Berlin)', parse_dates=True)
    historic_load_profile = model.HistoricLoadProfile.from_dataframe(id=uuid.uuid4(), component=component, df=df)
    assert len(historic_load_profile.timestamps) == 4608
