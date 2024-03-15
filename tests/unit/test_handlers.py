import random
import uuid
from datetime import datetime, timedelta
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import MemoryUnitOfWork
from src.services.load_data import APILoadDataRetriever
from src.services.data_store import LocalDataStore
from src.domain import commands
from src.domain import model


def setup_test():
    bus = MessageBus()
    bus.setup(
        MemoryUnitOfWork(),
        APILoadDataRetriever(),
        LocalDataStore(),
    )
    return bus


class TestAddComponent:
    def test_for_new_component(self, location):
        bus = setup_test()
        bus.uow.locations.add(location)
        component = bus.handle(
            commands.CreateComponent(
                type="producer", malo="1", location_ref=str(location.id)
            )
        )
        assert bus.uow.components.get(str(component.id)) is not None
        assert bus.uow.committed


class TestAddHistoricLoadProfile:
    def test_for_new_historic_load_profile(self, location):
        bus = setup_test()
        bus.uow.locations.add(location)
        timestamps = list()
        component = bus.handle(
            commands.CreateComponent(
                type="producer", malo="1", location_ref=str(location.id)
            )
        )
        for dt in (datetime.now() - timedelta(minutes=15 * n) for n in range(1000)):
            timestamps.append(
                model.TimeStamp(datetime=dt, value=random.uniform(0.0, 1.9))
            )
        cmd = commands.AddHistoricLoadProfile(
            component_ref=str(component.id), timestamps=timestamps
        )
        bus.handle(cmd)
        historic_load_profile: model.HistoricLoadProfile = (
            bus.uow.historic_load_profiles.get_by_component_ref(str(component.id))
        )
        assert historic_load_profile is not None
        assert len(historic_load_profile.timestamps) == 1000
