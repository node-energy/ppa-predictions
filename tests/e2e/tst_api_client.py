import pytest
from alembic import command
from alembic.config import Config
from pathlib import Path
from fastapi.testclient import TestClient

from src import enums
from src.config import settings
from src.enums import State, PredictionReceiver
from src.main import app
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import SqlAlchemyUnitOfWork
from src.services.load_data_exchange.common import APILoadDataRetriever
from src.services.data_sender import DataSender
from tests.factories import LocationFactory, PredictionFactory, ProducerFactory
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
    def test_create_location(self, bus, setup_database):
        json = {
            "state": State.BERLIN.value,
            "alias": "New Location",
            "residual_short": {"number": "market_location-1"},
            "residual_long": {"number": "market_location-2"},
            "producers": [
                {
                    "market_location": {
                        "number": "market_location-3"
                    },
                    "prognosis_data_retriever": "impuls_energy_trading_sftp"
                }
            ],
            "settings": {"active_from": "2024-01-01", "active_until": None},
        }

        response = client.post("/locations/", json=json)
        assert response.status_code == 200
        json["producers"][0].update({"id": response.json()["producers"][0]["id"]})
        assert json.items() <= response.json().items()

    def test_update_location_settings(self, bus, setup_database):
        json = {
            "state": State.BERLIN.value,
            "alias": "New Location",
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
        client.post("/locations/", json={"state": State.BERLIN, "alias": "Location-1", "residual_short": {"number": "market_location-1"}, "settings": {"active_from": "2024-01-01"}})
        client.post("/locations/", json={"state": State.BERLIN, "alias": "Location-2", "residual_short": {"number": "market_location-2"}, "settings": {"active_from": "2024-01-01"}})
        response = client.get("/locations/")
        assert response.status_code == 200
        assert response.json()["total"] == 2

    def test_get_location(self, bus, setup_database):
        json = {"state": State.BERLIN.value, "alias": "Location-1", "residual_short": {"number": "market_location-1"}, "settings": {"active_from": "2024-01-01", "active_until": None}}
        post_response = client.post("/locations/", json=json)
        location_id = post_response.json()["id"]
        response = client.get(f"/locations/{location_id}/")
        assert response.status_code == 200
        assert json.items() <= response.json().items()

    def test_calculate_predictions(self, bus, setup_database):
        ...
        # todo

    def test_send_eigenverbrauch_predictions(self, bus, setup_database):
        settings.send_predictions_enabled = True
        # ARRANGE
        location = LocationFactory.build(
            producers=[
                ProducerFactory.build(prognosis_data_retriever=enums.DataRetriever.IMPULS_ENERGY_TRADING_SFTP)
            ],
            predictions=[
                PredictionFactory.build(
                    type=enums.PredictionType.CONSUMPTION,
                    receivers=[PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT]
                )
            ]
        )
        with bus.uow as uow:
            uow.locations.add(location)
            uow.commit()
        # ACT
        response = client.post("/locations/send_eigenverbrauchs_predictions_impuls/")

        # ASSERT
        assert response.status_code == 202
        assert len(bus.dts.impuls_energy_trading_eigenverbrauch_sender.data) == 1
