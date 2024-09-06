import datetime as dt
import pandas as pd
from pandas._testing import assert_frame_equal
from pandera.typing import DataFrame

from src import enums
from src.enums import PredictionReceiver, TransmissionSystemOperator
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import MemoryUnitOfWork
from src.services.load_data_exchange.common import AbstractLoadDataRetriever
from src.services.data_sender import DataSender
from src.domain import commands
from src.domain import model
from src.utils.dataframe_schemas import IetEigenverbrauchSchema, IetResidualLoadSchema
from src.utils.external_schedules import GATE_CLOSURE_INTERNAL_FAHRPLANMANAGEMENT
from src.utils.timezone import TIMEZONE_BERLIN, TIMEZONE_UTC
from tests.factories import LocationFactory, ProducerFactory, PredictionFactory, PredictionShipmentFactory
from tests.fakes import FakeEmailSender, FakeIetDataSender


ONE_HOUR_BEFORE_GATE_CLOSURE = dt.datetime.combine(
    dt.date.today(),
    GATE_CLOSURE_INTERNAL_FAHRPLANMANAGEMENT
) - dt.timedelta(hours=1)


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
        mask = (
            location.residual_short.historic_load_data.df.index.date >= location.settings.active_from) & (
            location.residual_short.historic_load_data.df.index.date < location.settings.active_until if location.settings.active_until else True
        )
        for prediction in location.predictions:
            pd.testing.assert_index_equal(location.residual_short.historic_load_data.df["value"][mask].index, prediction.df.index, check_names=False)

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
        assert_frame_equal(bus.dts.impuls_energy_trading_eigenverbrauch_sender.data[0], DataFrame[IetEigenverbrauchSchema](
                index=pd.DatetimeIndex(
                    data=pd.date_range(
                        start=location_1.predictions[0].df.index[0].astimezone(TIMEZONE_UTC),
                        end=location_1.predictions[0].df.index[-1].astimezone(TIMEZONE_UTC),
                        freq="15min",
                    ),
                    name="#timestamp",
                ),
                data={
                    f"{location_1.id}": (location_1.predictions[0].df["value"] / 1000).round(3),
                    f"{location_2.id}": (location_2.predictions[0].df["value"] / 1000).round(3),
                }
            )
        )

        with bus.uow as uow:
            for id_ in [location_1.id, location_2.id]:
                prediction = uow.locations.get(id_).predictions[0]
                assert [s.receiver for s in prediction.shipments] == [PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT, PredictionReceiver.IMPULS_ENERGY_TRADING]

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
        assert_frame_equal(bus.dts.impuls_energy_trading_eigenverbrauch_sender.data[0], DataFrame[IetEigenverbrauchSchema](
                index=pd.DatetimeIndex(
                    data=pd.date_range(
                        start=location.predictions[0].df.index[0].astimezone(TIMEZONE_UTC),
                        end=location.predictions[0].df.index[-1].astimezone(TIMEZONE_UTC),
                        freq="15min",
                    ),
                    name="#timestamp",
                ),
                data={
                    f"{location.id}": (location.predictions[0].df["value"] / 1000).round(3),
                }
            )
        )

        with bus.uow as uow:
            prediction = uow.locations.get(location.id).predictions[0]
            assert [s.receiver for s in prediction.shipments] == [PredictionReceiver.IMPULS_ENERGY_TRADING]

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
        location_3 = LocationFactory.build(
            tso=TransmissionSystemOperator.TENNET,
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
            uow.locations.add(location_3)
            uow.commit()

        # ACT
        bus.handle(commands.SendAllResidualLongPredictionsToImpuls())

        # ASSERT
        assert_frame_equal(bus.dts.impuls_energy_trading_residual_long_sender.data[0], DataFrame[IetResidualLoadSchema](
                index=pd.DatetimeIndex(
                    data=pd.date_range(
                        start=location_1.predictions[0].df.index[0].astimezone(TIMEZONE_UTC),
                        end=location_1.predictions[0].df.index[-1].astimezone(TIMEZONE_UTC),
                        freq="15min",
                    ),
                    name="#timestamp",
                ),
                data={
                    "Amprion": (
                        (location_1.predictions[0].df["value"] + location_2.predictions[0].df["value"]) / 1000
                    ).round(3),
                    "TenneT": (location_3.predictions[0].df["value"] / 1000).round(3),
                    "50Hertz": 0.0,
                    "TransnetBW": 0.0,
                }
            )
        )

        with bus.uow as uow:
            for id_ in [location_1.id, location_2.id, location_3.id]:
                prediction = uow.locations.get(id_).predictions[0]
                assert [s.receiver for s in prediction.shipments] == [
                    PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT,
                    PredictionReceiver.IMPULS_ENERGY_TRADING
                ]
