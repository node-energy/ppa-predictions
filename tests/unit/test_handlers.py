import random
import uuid
from datetime import datetime, timedelta
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import MemoryUnitOfWork
from src.domain import commands
from src.domain import model


def setup_test():
    bus = MessageBus()
    bus.setup(MemoryUnitOfWork())
    return bus


class TestAddComponent:
    def test_for_new_component(self):
        bus = setup_test()
        customer = model.Customer(ref=uuid.uuid4())
        location = model.Location(ref=uuid.uuid4(), state=model.State.berlin, customer=customer)
        component = bus.handle(commands.CreateComponent(type='producer', location_ref=str(location.ref)))
        assert bus.uow.components.get(str(component.ref)) is not None
        assert bus.uow.committed


class TestAddHistoricLoadProfile:
    def test_for_new_historic_load_profile(self):
        bus = setup_test()
        timestamps = list()
        customer = model.Customer(ref=uuid.uuid4())
        location = model.Location(ref=uuid.uuid4(), state=model.State.berlin, customer=customer)
        component = bus.handle(commands.CreateComponent(type='producer', location_ref=str(location.ref)))
        for dt in (datetime.now() - timedelta(minutes=15*n) for n in range(1000)):
            timestamps.append(model.TimeStamp(datetime=dt, value=random.uniform(0.0, 1.9)))
        cmd = commands.AddHistoricLoadProfile(component_id=str(component.ref), timestamps=timestamps)
        bus.handle(cmd)
        historic_load_profile: model.HistoricLoadProfile = bus.uow.historic_load_profiles.get_by_component_ref(
            str(component.ref)
        )
        assert historic_load_profile is not None
        assert len(historic_load_profile.timestamps) == 1000
