import datetime
import datetime as dt
from unittest.mock import patch

import pandas as pd
from pandas._testing import assert_frame_equal
from pandera.typing import DataFrame

from src import enums
from src.enums import PredictionReceiver, TransmissionSystemOperator, PredictionType, DataRetriever
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import MemoryUnitOfWork
from src.services.load_data_exchange.common import AbstractLoadDataRetriever
from src.services.data_sender import DataSender
from src.domain import commands
from src.domain import model
from src.services.load_data_exchange.data_retriever_config import DATA_RETRIEVER_MAP, DataRetrieverConfig
from src.utils.dataframe_schemas import IetLoadDataSchema
from src.utils.timezone import TIMEZONE_BERLIN, TIMEZONE_UTC
from tests.conftest import ONE_HOUR_BEFORE_GATE_CLOSURE
from tests.factories import LocationFactory, ProducerFactory, PredictionFactory, PredictionShipmentFactory
from tests.fakes import FakeEmailSender, FakeIetDataSender


def create_df_with_constant_values(value=42.0):
    start = dt.datetime.now().replace(microsecond=0, second=0, minute=0, tzinfo=TIMEZONE_BERLIN)
    end = start + dt.timedelta(days=30)
    df = pd.DataFrame(
        index=pd.date_range(start=start, end=end, freq="15min"), data={"value": value}
    )
    return df


class FakeLoadDataRetriever(AbstractLoadDataRetriever):
    def get_data(self, asset_identifier: str, measurand: enums.Measurand, **kwargs):
        return create_df_with_constant_values()


def setup_test():
    bus = MessageBus()
    bus.setup(
        MemoryUnitOfWork(),
        FakeLoadDataRetriever(),
        dts=DataSender(
            fahrplanmanagement_sender=FakeEmailSender(),
            impuls_energy_trading_eigenverbrauch_sender=FakeIetDataSender(),
            impuls_energy_trading_residual_long_sender=FakeIetDataSender(),
        ),
    )
    return bus


class TestHistoricData:
    def test_update_historic_data_consumer_only(self):
        bus = setup_test()
        location = LocationFactory.build()

        location.residual_short = model.MarketLocation(number="MALO-CONSUMER-01", measurand=enums.Measurand.POSITIVE)
        bus.uow.locations.add(location)
        bus.handle(commands.UpdateHistoricData(location_id=str(location.id)))

        assert location.residual_short.historic_load_data is not None
        assert (
            location.residual_short.historic_load_data.df["value"] == 42
        ).all() == True

    def test_update_historic_data_with_production(self):
        bus = setup_test()
        location = LocationFactory.build()

        bus.uow.locations.add(location)
        bus.handle(commands.UpdateHistoricData(location_id=str(location.id)))

        assert location.residual_short.historic_load_data is not None
        assert location.residual_long.historic_load_data is not None
        assert location.producers.pop().market_location.historic_load_data is not None


class TestPrediction:
    def test_calculate_prediction_consumer_only(self):
        bus = setup_test()
        location = LocationFactory.build(producers=[])

        bus.uow.locations.add(location)
        bus.handle(commands.CalculatePredictions(location_id=str(location.id)))

        assert len(location.predictions) == 2

    def test_calculate_predictions_respects_start_and_end_ranges(self):
        today = dt.date.today()
        active_from = today + dt.timedelta(days=2)
        active_until = today + dt.timedelta(days=4)
        location = LocationFactory.build(
            producers=[],
            residual_long=None,
            settings__active_from=active_from,
            settings__active_until=active_until,
        )

        bus = setup_test()
        bus.uow.locations.add(location)
        bus.handle(commands.CalculatePredictions(location_id=str(location.id)))

        assert len(location.predictions) == 2
        index = pd.date_range(
            start=datetime.datetime.combine(location.settings.active_from, datetime.time(tzinfo=TIMEZONE_BERLIN)),
            end=datetime.datetime.combine(location.settings.active_until, datetime.time(tzinfo=TIMEZONE_BERLIN)),
            freq="15min",
            inclusive="left"
        )

        for prediction in location.predictions:
            pd.testing.assert_index_equal(pd.DatetimeIndex(index), prediction.df.index, check_names=False)

    def test_wont_calculate_predictions_if_not_active_yet(self):
        today = dt.date.today()
        active_from = today + dt.timedelta(days=50)
        location = LocationFactory.build(
            producers=[],
            residual_long=None,
            settings__active_from=active_from,
        )

        bus = setup_test()
        bus.uow.locations.add(location)
        bus.handle(commands.CalculatePredictions(location_id=str(location.id)))

        assert len(location.predictions) == 0

    def test_calculate_prediction_teileinspeiser(self):
        with patch.dict(
                DATA_RETRIEVER_MAP,
                {DataRetriever.ENERCAST_SFTP: DataRetrieverConfig(
                    FakeLoadDataRetriever,
                    lambda location: location.residual_long.number,
                )},
                clear=True
        ):
            bus = setup_test()
            location = LocationFactory.build()

            bus.uow.locations.add(location)
            bus.handle(commands.CalculatePredictions(location_id=str(location.id)))

        assert len(location.predictions) == 4
        assert sorted([p.type for p in location.predictions]) == sorted([
            PredictionType.CONSUMPTION,
            PredictionType.PRODUCTION,
            PredictionType.RESIDUAL_SHORT,
            PredictionType.RESIDUAL_LONG,
        ])

        short_prediction = next(filter(lambda p: p.type == PredictionType.RESIDUAL_SHORT, location.predictions))
        consumption_prediction = next(filter(lambda p: p.type == PredictionType.CONSUMPTION, location.predictions))
        long_prediction = next(filter(lambda p: p.type == PredictionType.RESIDUAL_LONG, location.predictions))
        producer_prediction = next(filter(lambda p: p.type == PredictionType.PRODUCTION and p.component == location.producers[0], location.predictions))

        expected_residual = producer_prediction.df - consumption_prediction.df
        expected_residual = expected_residual[expected_residual.first_valid_index(): expected_residual.last_valid_index()]
        expected_residual_long = expected_residual.clip(lower=0)
        expected_residual_short = expected_residual.clip(upper=0) * -1

        assert_frame_equal(long_prediction.df, expected_residual_long)
        assert_frame_equal(short_prediction.df, expected_residual_short)

    def test_calculate_prediction_teileinspeiser_multiple_producers(self):
        with patch.dict(
                DATA_RETRIEVER_MAP,
                {DataRetriever.ENERCAST_SFTP: DataRetrieverConfig(
                    FakeLoadDataRetriever,
                    lambda location: location.residual_long.number,
                )},
                clear=True
        ):
            bus = setup_test()
            producers = [
                ProducerFactory.build(),
                ProducerFactory.build(),
            ]
            location = LocationFactory.build(
                producers=producers,
            )

            bus.uow.locations.add(location)
            bus.handle(commands.CalculatePredictions(location_id=str(location.id)))

        assert len(location.predictions) == 5
        assert sorted([p.type for p in location.predictions]) == sorted([
            PredictionType.CONSUMPTION,
            PredictionType.PRODUCTION,
            PredictionType.PRODUCTION,
            PredictionType.RESIDUAL_SHORT,
            PredictionType.RESIDUAL_LONG,
        ])

        short_prediction = next(filter(lambda p: p.type == PredictionType.RESIDUAL_SHORT, location.predictions))
        consumption_prediction = next(filter(lambda p: p.type == PredictionType.CONSUMPTION, location.predictions))
        long_prediction = next(filter(lambda p: p.type == PredictionType.RESIDUAL_LONG, location.predictions))
        producer_1_prediction = next(filter(lambda p: p.type == PredictionType.PRODUCTION and p.component == producers[0], location.predictions))
        producer_2_prediction = next(filter(lambda p: p.type == PredictionType.PRODUCTION and p.component == producers[1], location.predictions))

        expected_residual = producer_1_prediction.df + producer_2_prediction.df - consumption_prediction.df
        expected_residual = expected_residual[expected_residual.first_valid_index(): expected_residual.last_valid_index()]
        expected_residual_long = expected_residual.clip(lower=0)
        expected_residual_short = expected_residual.clip(upper=0) * -1

        assert_frame_equal(long_prediction.df, expected_residual_long)
        assert_frame_equal(short_prediction.df, expected_residual_short)


class TestSendPredictions:
    def test_send_eigenverbrauch_predictions_to_impuls_energy_trading(self):
        # ARRANGE
        bus = setup_test()

        location_1 = LocationFactory.build(
            producers=[
                ProducerFactory.build(prognosis_data_retriever=enums.DataRetriever.IMPULS_ENERGY_TRADING_SFTP)
            ],
            predictions=[
                PredictionFactory.build(
                    type=enums.PredictionType.CONSUMPTION,
                    shipments=[
                        PredictionShipmentFactory.build(
                            created=ONE_HOUR_BEFORE_GATE_CLOSURE,
                            receiver=PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT
                        )
                    ]
                ),
                PredictionFactory.build(
                    type=enums.PredictionType.PRODUCTION,
                    shipments=[
                        PredictionShipmentFactory.build(
                            created=ONE_HOUR_BEFORE_GATE_CLOSURE,
                            receiver=PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT
                        )
                    ]
                )
            ]
        )
        location_2 = LocationFactory.build(
            producers=[
                ProducerFactory.build(prognosis_data_retriever=enums.DataRetriever.IMPULS_ENERGY_TRADING_SFTP)
            ],
            predictions=[
                PredictionFactory.build(
                    type=enums.PredictionType.CONSUMPTION,
                    shipments=[
                        PredictionShipmentFactory.build(
                            created=ONE_HOUR_BEFORE_GATE_CLOSURE,
                            receiver=PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT
                        )
                    ]
                ),
                PredictionFactory.build(
                    type=enums.PredictionType.PRODUCTION,
                    shipments=[
                        PredictionShipmentFactory.build(
                            created=ONE_HOUR_BEFORE_GATE_CLOSURE,
                            receiver=PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT
                        )
                    ]
                )
            ]
        )
        # not applicable for impuls energy trading
        location_3 = LocationFactory.build(
            producers=[
                ProducerFactory.build(prognosis_data_retriever=enums.DataRetriever.ENERCAST_SFTP)
            ],
            predictions=[
                PredictionFactory.build(
                    type=enums.PredictionType.CONSUMPTION,
                    shipments=[
                        PredictionShipmentFactory.build(
                            created=ONE_HOUR_BEFORE_GATE_CLOSURE,
                            receiver=PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT
                        )
                    ]
                )
            ]
        )
        with bus.uow as uow:
            uow.locations.add(location_1)
            uow.locations.add(location_2)
            uow.locations.add(location_3)
            uow.commit()

        # ACT
        bus.handle(commands.SendAllEigenverbrauchsPredictionsToImpuls())

        # ASSERT
        for day in pd.date_range(datetime.date.today() + datetime.timedelta(days=1), freq="D", periods=6):
            day = day.to_pydatetime().date()

            consumption_prediction = next(filter(lambda p: p.type == PredictionType.CONSUMPTION, location_1.predictions)).df
            production_prediction = next(filter(lambda p: p.type == PredictionType.PRODUCTION, location_1.predictions)).df
            df1 = consumption_prediction.clip(upper=production_prediction)
            df1 = (df1.tz_convert(TIMEZONE_BERLIN) / 1000).round(3)
            df1 = df1[df1.index.date == day]
            df1 = df1.tz_convert(TIMEZONE_UTC)

            consumption_prediction = next(filter(lambda p: p.type == PredictionType.CONSUMPTION, location_2.predictions)).df
            production_prediction = next(filter(lambda p: p.type == PredictionType.PRODUCTION, location_2.predictions)).df
            df2 = consumption_prediction.clip(upper=production_prediction)
            df2 = (df2.tz_convert(TIMEZONE_BERLIN) / 1000).round(3)
            df2 = df2[df2.index.date == day]
            df2 = df2.tz_convert(TIMEZONE_UTC)
            df = pd.concat([df1, df2], axis=1)
            df.index.name = "#timestamp"
            df.columns = [f"{location_1.residual_long.id}", f"{location_2.residual_long.id}"]
            df = DataFrame[IetLoadDataSchema](df)

            assert_frame_equal(bus.dts.impuls_energy_trading_eigenverbrauch_sender.data[day], df)

    def test_enforce_sending_eigenverbrauch_predictions_to_impuls_energy_trading(self):
        # ARRANGE
        bus = setup_test()

        location = LocationFactory.build(
            producers=[
                ProducerFactory.build(prognosis_data_retriever=enums.DataRetriever.IMPULS_ENERGY_TRADING_SFTP)
            ],
            predictions=[
                PredictionFactory.build(
                    type=enums.PredictionType.CONSUMPTION,
                ),
                PredictionFactory.build(
                    type=enums.PredictionType.PRODUCTION,
                )
            ]
        )
        with bus.uow as uow:
            uow.locations.add(location)

        # ACT
        bus.handle(commands.SendAllEigenverbrauchsPredictionsToImpuls(
            send_even_if_not_sent_to_internal_fahrplanmanagement=True
        ))

        # ASSERT
        for day in pd.date_range(datetime.date.today() + datetime.timedelta(days=1), freq="D", periods=6):
            day = day.to_pydatetime().date()

            df = (location.predictions[0].df.tz_convert(TIMEZONE_BERLIN) / 1000).round(3)
            df = df[df.index.date == day]
            df = df.tz_convert(TIMEZONE_UTC)
            df.index.name = "#timestamp"
            df.columns = [f"{location.residual_long.id}"]
            df = DataFrame[IetLoadDataSchema](df)
            assert_frame_equal(bus.dts.impuls_energy_trading_eigenverbrauch_sender.data[day], df)

    def test_send_residual_long_predictions_to_impuls_energy_trading(self):
        # ARRANGE
        bus = setup_test()

        location_1 = LocationFactory.build(
            tso=TransmissionSystemOperator.AMPRION,
            producers=[
                ProducerFactory.build(prognosis_data_retriever=enums.DataRetriever.IMPULS_ENERGY_TRADING_SFTP)
            ],
            predictions=[
                PredictionFactory.build(
                    type=enums.PredictionType.RESIDUAL_LONG,
                    shipments=[
                        PredictionShipmentFactory.build(
                            created=ONE_HOUR_BEFORE_GATE_CLOSURE,
                            receiver=PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT
                        )
                    ]
                )
            ]
        )
        location_2 = LocationFactory.build(
            tso=TransmissionSystemOperator.AMPRION,
            producers=[
                ProducerFactory.build(prognosis_data_retriever=enums.DataRetriever.IMPULS_ENERGY_TRADING_SFTP)
            ],
            predictions=[
                PredictionFactory.build(
                    type=enums.PredictionType.RESIDUAL_LONG,
                    shipments=[
                        PredictionShipmentFactory.build(
                            created=ONE_HOUR_BEFORE_GATE_CLOSURE,
                            receiver=PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT
                        )
                    ]
                )
            ]
        )
        with bus.uow as uow:
            uow.locations.add(location_1)
            uow.locations.add(location_2)
            uow.commit()

        # ACT
        bus.handle(commands.SendAllResidualLongPredictionsToImpuls())

        # ASSERT
        for day in pd.date_range(datetime.date.today() + datetime.timedelta(days=1), freq="D", periods=6):
            day = day.to_pydatetime().date()

            df1 = (location_1.predictions[0].df.tz_convert(TIMEZONE_BERLIN) / 1000).round(3)
            df1 = df1[df1.index.date == day]
            df1 = df1.tz_convert(TIMEZONE_UTC)
            df2 = (location_2.predictions[0].df.tz_convert(TIMEZONE_BERLIN) / 1000).round(3)
            df2 = df2[df2.index.date == day]
            df2 = df2.tz_convert(TIMEZONE_UTC)
            df = pd.concat([df1, df2], axis=1)
            df.index.name = "#timestamp"
            df.columns = [f"{location_1.residual_long.id}", f"{location_2.residual_long.id}"]
            df = DataFrame[IetLoadDataSchema](df)

            assert_frame_equal(bus.dts.impuls_energy_trading_residual_long_sender.data[day], df)

        with bus.uow as uow:
            for id_ in [location_1.id, location_2.id]:
                prediction = uow.locations.get(id_).predictions[0]
                assert [s.receiver for s in prediction.shipments] == [
                    PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT,
                    PredictionReceiver.IMPULS_ENERGY_TRADING
                ]
