from __future__ import annotations
import inspect
from typing import Union
from src.domain import events, commands
from src.infrastructure import unit_of_work
from src.domain.handlers import COMMAND_HANDLERS, EVENT_HANDLERS
from src.services import load_data, data_store


Message = Union[commands.Command, events.Event]


def inject_dependencies(handler, dependencies):
    params = inspect.signature(handler).parameters
    deps = {
        name: dependency
        for name, dependency in dependencies.items()
        if name in params
    }
    return lambda message: handler(message, **deps)


class MessageBus:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MessageBus, cls).__new__(cls)
        return cls._instance

    def setup(
        self,
        uow: unit_of_work.AbstractUnitOfWork,
        ldr: load_data.AbstractLoadDataRetriever,
        dst: data_store.AbstractDataStore,
    ):
        self.uow = uow
        self.ldr = ldr
        self.dst = dst
        dependencies = {"uow": uow, "ldr": ldr, "dst": dst}
        self.command_handlers = {
            command_type: inject_dependencies(handler, dependencies)
            for command_type, handler in COMMAND_HANDLERS.items()
        }
        self.event_handlers = {
            event_type: [
                inject_dependencies(handler, dependencies)
                for handler in event_handlers
            ]
            for event_type, event_handlers in EVENT_HANDLERS.items()
        }

    def handle(self, message: Message):
        self.queue = [message]
        while self.queue:
            message = self.queue.pop(0)
            if isinstance(message, commands.Command):
                return self.handle_command(message)
            elif isinstance(message, events.Event):
                self.handle_event(message)
            else:
                raise Exception()

    def handle_command(self, command: commands.Command):
        try:
            handler = self.command_handlers[type(command)]
            result = handler(command)
            self.queue.extend(self.uow.collect_new_events())
            return result
        except Exception:
            raise

    def handle_event(self, event: events.Event):
        for handler in self.event_handlers[type(event)]:
            try:
                handler(event)
                self.queue.extend(self.uow.collect_new_events())
            except Exception:
                continue
