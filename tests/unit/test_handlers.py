import datetime as dt
import pandas as pd

from src import enums
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import MemoryUnitOfWork
from src.services.load_data import AbstractLoadDataRetriever
from src.services.data_store import LocalDataStore
from src.domain import commands
from src.domain import model
from src.utils.timezone import TIMEZONE_BERLIN
from tests.unit.conftest import random_malo


def create_df_with_constant_values(value=42.0):
    start = dt.datetime.now().replace(microsecond=0, second=0, minute=0, tzinfo=TIMEZONE_BERLIN)
    end = start + dt.timedelta(days=30)
    df = pd.DataFrame(
        index=pd.date_range(start=start, end=end, freq="15min"), data={"value": value}
    )
    return df


class FakeLoadDataReceiver(AbstractLoadDataRetriever):
    def get_data(self, asset_identifier: str, measurand: enums.Measurand):
        return create_df_with_constant_values()


def setup_test():
    bus = MessageBus()
    bus.setup(
        MemoryUnitOfWork(),
        FakeLoadDataReceiver(),
        LocalDataStore(),
    )
    return bus


class TestHistoricData:
    def test_update_historic_data_consumer_only(self, location):
        bus = setup_test()

        location.residual_short = model.MarketLocation(number="MALO-CONSUMER-01", measurand=enums.Measurand.POSITIVE)
        bus.uow.locations.add(location)
        bus.handle(commands.UpdateHistoricData(location_id=str(location.id)))

        assert location.residual_short.historic_load_data is not None
        assert (
            location.residual_short.historic_load_data.df["value"] == 42
        ).all() == True

    def test_update_historic_data_with_production(self, location):
        bus = setup_test()
        location.residual_short = model.MarketLocation(number="MALO-CONSUMER-01", measurand=enums.Measurand.POSITIVE)
        location.residual_long = model.MarketLocation(number="MALO-PRODUCTION-01", measurand=enums.Measurand.NEGATIVE)
        producer1 = model.Producer(market_location=model.MarketLocation(number="MALO-PRODUCER-01", measurand=enums.Measurand.NEGATIVE), prognosis_data_retriever=enums.DataRetriever.ENERCAST_SFTP)
        location.producers.append(producer1)

        bus.uow.locations.add(location)
        bus.handle(commands.UpdateHistoricData(location_id=str(location.id)))

        assert location.residual_short.historic_load_data is not None
        assert location.residual_long.historic_load_data is not None
        assert location.producers.pop().market_location.historic_load_data is not None


class TestPrediction:
    def test_calculate_prediction_consumer_only(self, location):
        bus = setup_test()
        historic_load_data = model.HistoricLoadData(df=create_df_with_constant_values())
        location.residual_short = model.MarketLocation(number="MALO-CONSUMER-01", measurand=enums.Measurand.POSITIVE, historic_load_data=historic_load_data)
        bus.uow.locations.add(location)
        bus.handle(commands.CalculatePredictions(location_id=str(location.id)))

        assert len(location.predictions) == 2

    def test_calculate_predictions_respects_start_and_end_ranges(self):
        today = dt.date.today()
        active_from = today + dt.timedelta(days=2)
        active_until = today + dt.timedelta(days=4)
        location = self.create_location_with_settings(active_from, active_until)

        bus = setup_test()
        historic_load_data = model.HistoricLoadData(df=create_df_with_constant_values())
        location.residual_short = model.MarketLocation(number="MALO-CONSUMER-01", measurand=enums.Measurand.POSITIVE, historic_load_data=historic_load_data)
        bus.uow.locations.add(location)
        bus.handle(commands.CalculatePredictions(location_id=str(location.id)))

        assert len(location.predictions) == 2
        for prediction in location.predictions:
            mask = (
                historic_load_data.df.index.date >= location.settings.active_from) & (
                historic_load_data.df.index.date < location.settings.active_until if location.settings.active_until else True
            )
            pd.testing.assert_series_equal(historic_load_data.df["value"][mask], prediction.df.squeeze(), check_names=False)

    def test_wont_calculate_predictions_if_not_active_yet(self):
        today = dt.date.today()
        active_from = today + dt.timedelta(days=50)
        location = self.create_location_with_settings(active_from, None)

        bus = setup_test()
        historic_load_data = model.HistoricLoadData(df=create_df_with_constant_values())
        location.residual_short = model.MarketLocation(number="MALO-CONSUMER-01", measurand=enums.Measurand.NEGATIVE, historic_load_data=historic_load_data)
        bus.uow.locations.add(location)
        bus.handle(commands.CalculatePredictions(location_id=str(location.id)))

        assert len(location.predictions) == 0

    @staticmethod
    def create_location_with_settings(active_from, active_until) -> model.Location:
        return model.Location(
            settings=model.LocationSettings(
                active_from=active_from,
                active_until=active_until,
            ),
            state=enums.State.berlin,
            residual_short=model.MarketLocation(number=random_malo(), measurand=enums.Measurand.POSITIVE),
        )