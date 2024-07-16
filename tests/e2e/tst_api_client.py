import pytest
from alembic import command
from alembic.config import Config
from pathlib import Path
from uuid import uuid4
from fastapi.testclient import TestClient
from src.main import app
from src.domain.model import Project
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import SqlAlchemyUnitOfWork
from src.services.load_data import APILoadDataRetriever
from src.services.data_store import LocalDataStore

#
# client = TestClient(app)
# ALEMBIC_BASE_PATH = Path(__file__).parent.parent.parent.resolve()
#
#
# @pytest.fixture
# def bus():
#     bus = MessageBus()
#     bus.setup(
#         uow=SqlAlchemyUnitOfWork(),
#         ldr=APILoadDataRetriever(),
#         dst=LocalDataStore(),
#     )
#     return bus
#
#
# @pytest.fixture
# def setup_database():  # TODO Hack
#     alembic_cfg = Config(ALEMBIC_BASE_PATH.joinpath("alembic.ini"))
#     alembic_cfg.set_main_option(
#         "script_location", str(ALEMBIC_BASE_PATH.joinpath("alembic"))
#     )
#     command.downgrade(alembic_cfg, "base")
#     command.upgrade(alembic_cfg, "head")
#
#
# class TestProject:
#     def test_create_project(self, bus, setup_database):
#         response = client.post("/projects/", json={"name": "New Project"})
#         assert response.status_code == 201
#         assert {"name": "New Project"}.items() <= response.json().items()
#
#     def test_get_projects(self, bus, setup_database):
#         client.post("/projects/", json={"name": "New Project"})
#         client.post("/projects/", json={"name": "New Project"})
#         response = client.get("/projects/")
#         assert response.status_code == 200
#         assert response.json()["total"] == 2
#
#     def test_get_project(self, bus, setup_database):
#         project = Project(id=uuid4(), name="New Project")
#         with bus.uow as uow:
#             uow.projects.add(project)
#             uow.commit()
#         response = client.get(f"/projects/{project.id}/")
#         assert response.status_code == 200
#         assert response.json() == {"id": str(project.id), "name": project.name}
#
#     def test_get_project_invalid_id(self, bus):
#         response = client.get(f"/projects/{uuid4()}/")
#         assert response.status_code == 404
#
#     def test_update_project(self, bus, setup_database):
#         project = Project(id=uuid4(), name="New Project")
#         with bus.uow as uow:
#             uow.projects.add(project)
#             uow.commit()
#         response = client.patch(
#             f"/projects/{project.id}/", json={"name": "Renamed Project"}
#         )
#         assert response.status_code == 200
#         assert {"name": "Renamed Project"}.items() <= response.json().items()
#         updated_project = uow.projects.get(project.id)
#         assert updated_project.name == "Renamed Project"
#         assert str(updated_project.id) == str(project.id)
#
#
# class TestLocation:
#     def test_create_location(self, bus, setup_database):
#         response = client.post("/locations/", json={"name": "New Location"})
#         assert response.status_code == 201
#         assert {"name": "New Location"}.items() <= response.json().items()
