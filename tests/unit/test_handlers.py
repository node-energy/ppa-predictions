import datetime as dt
import pandas as pd
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import MemoryUnitOfWork
from src.services.load_data import AbstractLoadDataRetriever
from src.services.data_store import LocalDataStore
from src.domain import commands
from src.domain import model
from tests.unit.conftest import random_malo


def create_df_with_constant_values(value=42.0):
    start = dt.datetime.now().replace(microsecond=0, second=0, minute=0)
    end = start + dt.timedelta(days=30)
    df = pd.DataFrame(
        index=pd.date_range(start=start, end=end, freq="15min"), data={"value": value}
    )
    return df


class FakeLoadDataReceiver(AbstractLoadDataRetriever):
    def get_data(self, malo: str):
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

        location.residual_short = model.Consumer(malo="MALO-CONSUMER-01")
        bus.uow.locations.add(location)
        bus.handle(commands.UpdateHistoricData(location_id=str(location.id)))

        assert location.residual_short.historic_load_data is not None
        assert (
            location.residual_short.historic_load_data.df["value"] == 42
        ).all() == True

    def test_update_historic_data_with_production(self, location):
        bus = setup_test()
        location.residual_short = model.Consumer(malo="MALO-CONSUMER-01")
        location.residual_long = model.Consumer(malo="MALO-PRODUCTION-01")
        producer1 = model.Producer(malo="MALO-PRODUCER-01")
        location.producers.append(producer1)

        bus.uow.locations.add(location)
        bus.handle(commands.UpdateHistoricData(location_id=str(location.id)))

        assert location.residual_short.historic_load_data is not None
        assert location.residual_long.historic_load_data is not None
        assert location.producers.pop().historic_load_data is not None


class TestPrediction:
    def test_calculate_prediction_consumer_only(self, location):
        bus = setup_test()
        historic_load_data = model.HistoricLoadData(df=create_df_with_constant_values())
        location.residual_short = model.Consumer(malo="MALO-CONSUMER-01", historic_load_data=historic_load_data)
        bus.uow.locations.add(location)
        bus.handle(commands.CalculatePredictions(location_id=str(location.id)))

        assert len(location.predictions) == 2

    def test_calculate_predictions_respsects_start_and_end_ranges(self):
        today = dt.datetime.combine(
            dt.datetime.now().replace(hour=0, minute=0, second=0),
            dt.time.min
        )
        active_from = today + dt.timedelta(days=2)
        active_until = today + dt.timedelta(days=4)
        location = self.create_location_with_settings(active_from, active_until)

        bus = setup_test()
        historic_load_data = model.HistoricLoadData(df=create_df_with_constant_values())
        location.residual_short = model.Consumer(malo="MALO-CONSUMER-01", historic_load_data=historic_load_data)
        bus.uow.locations.add(location)
        bus.handle(commands.CalculatePredictions(location_id=str(location.id)))

        assert len(location.predictions) == 2
        for prediction in location.predictions:
            mask = (
                historic_load_data.df.index >= location.settings.active_from) & (
                historic_load_data.df.index < location.settings.active_until if location.settings.active_until else True
            )
            pd.testing.assert_series_equal(historic_load_data.df["value"][mask], prediction.df.squeeze(), check_names=False)

    def test_wont_calculate_predictions_if_not_active_yet(self):
        today = dt.datetime.combine(
            dt.datetime.now().replace(hour=0, minute=0, second=0),
            dt.time.min
        )
        active_from = today + dt.timedelta(days=50)
        location = self.create_location_with_settings(active_from, None)

        bus = setup_test()
        historic_load_data = model.HistoricLoadData(df=create_df_with_constant_values())
        location.residual_short = model.Consumer(malo="MALO-CONSUMER-01", historic_load_data=historic_load_data)
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
            state=model.State.berlin,
            residual_short=model.Consumer(malo=random_malo()),
        )