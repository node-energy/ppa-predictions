import pytest
from alembic import command
from alembic.config import Config
from pathlib import Path
from fastapi.testclient import TestClient
from freezegun import freeze_time

from src import enums
from src.config import settings
from src.enums import State, PredictionReceiver, TransmissionSystemOperator
from src.main import app
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import SqlAlchemyUnitOfWork
from src.services.load_data_exchange.common import APILoadDataRetriever
from src.services.data_sender import DataSender
from tests.conftest import ONE_HOUR_BEFORE_GATE_CLOSURE
from tests.factories import LocationFactory, PredictionFactory, ProducerFactory, PredictionShipmentFactory
from tests.fakes import FakeEmailSender, FakeIetDataSender

client = TestClient(app, headers={"X-Api-Key": settings.api_key})
ALEMBIC_BASE_PATH = Path(__file__).parent.parent.parent.resolve()


@pytest.fixture
def bus():
    bus = MessageBus()
    bus.setup(
        uow=SqlAlchemyUnitOfWork(),
        ldr=APILoadDataRetriever(),
        dts=DataSender(
            fahrplanmanagement_sender=FakeEmailSender(),
            impuls_energy_trading_eigenverbrauch_sender=FakeIetDataSender(),
            impuls_energy_trading_residual_long_sender=FakeIetDataSender(),
        ),
    )
    return bus


@pytest.fixture
def setup_database():  # TODO Hack
    alembic_cfg = Config(ALEMBIC_BASE_PATH.joinpath("alembic.ini"))
    alembic_cfg.set_main_option(
        "script_location", str(ALEMBIC_BASE_PATH.joinpath("alembic"))
    )
    command.downgrade(alembic_cfg, "base")
    command.upgrade(alembic_cfg, "head")


class TestLocation:
    @pytest.mark.parametrize(
        "json",
        [
            pytest.param({
                    "state": State.BERLIN.value,
                    "tso": TransmissionSystemOperator.AMPRION.value,
                    "residual_short": {"number": "market_location-1"},
                    "settings": {"active_from": "2024-01-01"},
                }, id="only mandatory fields"
            ),
            pytest.param({
                    "state": State.BERLIN.value,
                    "alias": "New Location",
                    "tso": TransmissionSystemOperator.AMPRION.value,
                    "residual_short": {"number": "market_location-1"},
                    "residual_long": {"number": "market_location-2"},
                    "producers": [
                        {
                            "name": "Producer 1",
                            "market_location": {
                                "number": "market_location-3"
                            },
                            "prognosis_data_retriever": "impuls_energy_trading_sftp"
                        }
                    ],
                    "settings": {"active_from": "2024-01-01", "active_until": None},
                }, id="all fields but no ids specified"
            ),
            pytest.param({
                    "id": "a0125769-fde8-47f9-87ab-0a9c7cc4ee00",
                    "state": State.BERLIN.value,
                    "alias": "New Location",
                    "tso": TransmissionSystemOperator.AMPRION.value,
                    "residual_short": {"id": "942ca59d-745c-492f-9095-d58cba45120d", "number": "market_location-1"},
                    "residual_long": {"id": "ec489260-e178-4add-89c3-2166639218ac", "number": "market_location-2"},
                    "producers": [
                        {
                            "id": "8d1cc110-fa2b-4fcc-a404-80155b431649",
                            "name": "Producer 1",
                            "market_location": {
                                "id": "49214c9c-1474-4ac2-9bcc-d7a9c3c999d7",
                                "number": "market_location-3"
                            },
                            "prognosis_data_retriever": "impuls_energy_trading_sftp"
                        }
                    ],
                    "settings": {"active_from": "2024-01-01", "active_until": None},
                }, id="all fields including all possible ids"
            ),
        ]
    )
    def test_create_location(self, bus, setup_database, json):
        response = client.post("/locations/", json=json)
        assert response.status_code == 200

        expected_json = {
            'id': json["id"] if "id" in json.keys() else response.json()["id"],
            'state': 'BE',
            'alias': json["alias"] if "alias" in json.keys() else None,
            'tso': 'amprion',
            'residual_short': {
                'id': json['residual_short']["id"] if "id" in json['residual_short'].keys() else response.json()['residual_short']["id"],
                'number': 'market_location-1'
            },
            'residual_long': {
                'id': json['residual_long']['id'] if 'id' in json['residual_long'].keys() else response.json()['residual_long']['id'],
                'number': 'market_location-2'
            } if 'residual_long' in json.keys() else None,
            'producers': [
                {
                    'id': json['producers'][0]['id'] if 'id' in json['producers'][0].keys() else response.json()['producers'][0]['id'],
                    'name': 'Producer 1',
                    'market_location': {
                        'id': json['producers'][0]['market_location']['id'] if 'id' in json['producers'][0]['market_location'].keys() else response.json()['producers'][0]['market_location']['id'],
                        'number': 'market_location-3'
                    },
                    'prognosis_data_retriever': 'impuls_energy_trading_sftp'
                }
            ] if 'producers' in json.keys() else [
            ],
            'settings': {
                'active_from': '2024-01-01',
                'active_until': None
            }
        }

        assert expected_json == response.json()

    def test_update_location_settings(self, bus, setup_database):
        json = {
            "state": State.BERLIN.value,
            "alias": "New Location",
            "tso": TransmissionSystemOperator.AMPRION.value,
            "residual_short": {"number": "market_location-1"},
            "settings": {"active_from": "2024-01-01", "active_until": None},
        }

        post_response = client.post("/locations/", json=json)

        location_id = post_response.json()["id"]
        json = {"active_from": "2024-01-01", "active_until": "2024-01-10"}
        response = client.put(f"/locations/{location_id}/settings", json=json)
        assert response.status_code == 200
        assert response.json()["settings"]["active_until"] == "2024-01-10"

    def test_get_locations(self, bus, setup_database):
        client.post("/locations/", json={"state": State.BERLIN, "alias": "Location-1", "tso": TransmissionSystemOperator.AMPRION.value, "residual_short": {"number": "market_location-1"}, "settings": {"active_from": "2024-01-01"}})
        client.post("/locations/", json={"state": State.BERLIN, "alias": "Location-2", "tso": TransmissionSystemOperator.AMPRION.value, "residual_short": {"number": "market_location-2"}, "settings": {"active_from": "2024-01-01"}})
        response = client.get("/locations/")
        assert response.status_code == 200
        assert response.json()["total"] == 2

    def test_get_location(self, bus, setup_database):
        json = {"state": State.BERLIN.value, "alias": "Location-1", "tso": TransmissionSystemOperator.AMPRION.value, "residual_short": {"number": "market_location-1"}, "settings": {"active_from": "2024-01-01", "active_until": None}}
        post_response = client.post("/locations/", json=json)
        location_id = post_response.json()["id"]
        response = client.get(f"/locations/{location_id}/")
        assert response.status_code == 200

        expected_json = {
            'id': location_id,
            'state': 'BE',
            'alias': 'Location-1',
            'tso': 'amprion',
            'residual_short': {
                'id': response.json()['residual_short']['id'],
                'number': 'market_location-1'
            },
            'residual_long': None,
            'producers': [],
            'settings': {
                'active_from': '2024-01-01',
                'active_until': None
            }
        }
        assert expected_json == response.json()

    def test_calculate_predictions(self, bus, setup_database):
        ...
        # todo

    @freeze_time(ONE_HOUR_BEFORE_GATE_CLOSURE)
    def test_send_eigenverbrauch_predictions(self, bus, setup_database):
        # ARRANGE
        location = LocationFactory.build(
            producers=[
                ProducerFactory.build(prognosis_data_retriever=enums.DataRetriever.IMPULS_ENERGY_TRADING_SFTP)
            ],
            predictions=[
                PredictionFactory.build(
                    type=enums.PredictionType.CONSUMPTION,
                    shipments=[PredictionShipmentFactory.build(receiver=PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT)]
                )
            ]
        )
        with bus.uow as uow:
            uow.locations.add(location)
            uow.commit()
        # ACT
        response = client.post(
            "/locations/send_eigenverbrauchs_predictions_impuls/",
            json={}
        )

        # ASSERT
        assert response.status_code == 202
        assert len(bus.dts.impuls_energy_trading_eigenverbrauch_sender.data) == 8

    def test_enforce_sending_eigenverbrauch_predictions(self, bus, setup_database):
        # ARRANGE
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
            uow.commit()
        # ACT
        response = client.post(
            "/locations/send_eigenverbrauchs_predictions_impuls/",
            json={"send_even_if_not_sent_to_internal_fahrplanmanagement": True}
        )

        # ASSERT
        assert response.status_code == 202
        assert len(bus.dts.impuls_energy_trading_eigenverbrauch_sender.data) == 8

    @freeze_time(ONE_HOUR_BEFORE_GATE_CLOSURE)
    def test_send_residual_long_predictions(self, bus, setup_database):
        # ARRANGE
        location = LocationFactory.build(
            tso=TransmissionSystemOperator.AMPRION,
            producers=[
                ProducerFactory.build(prognosis_data_retriever=enums.DataRetriever.IMPULS_ENERGY_TRADING_SFTP)
            ],
            predictions=[
                PredictionFactory.build(
                    type=enums.PredictionType.RESIDUAL_LONG,
                    shipments=[
                        PredictionShipmentFactory.build(
                            receiver=PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT
                        )
                    ]
                )
            ]
        )
        with bus.uow as uow:
            uow.locations.add(location)
            uow.commit()
        # ACT
        response = client.post(
            "/locations/send_residual_long_predictions_impuls/",
            json={}
        )

        # ASSERT
        assert response.status_code == 202
        assert len(bus.dts.impuls_energy_trading_residual_long_sender.data) == 8

