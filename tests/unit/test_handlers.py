from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import MemoryUnitOfWork
from src.domain.commands import CreateComponent


def setup_test():
    bus = MessageBus()
    bus.setup(MemoryUnitOfWork())
    return bus


class TestAddComponent:
    def test_for_new_component(self):
        bus = setup_test()
        component = bus.handle(CreateComponent(type='producer'))
        assert bus.uow.components.get(str(component.ref)) is not None
        assert bus.uow.committed
