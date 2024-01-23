from __future__ import annotations
from typing import Callable, Dict, List, Type, Union
from src.domain import events, commands
from src.infrastructure import unit_of_work


Message = Union[commands.Command, events.Event]


class MessageBus:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(MessageBus, cls).__new__(cls)
        return cls._instance

    def setup(
        self,
        uow: unit_of_work.AbstractUnitOfWork,
        command_handlers: Dict[Type[commands.Command], Callable],
        event_handlers: Dict[Type[events.Event], List[Callable]]
    ):
        self.uow = uow
        self.command_handlers = command_handlers
        self.event_handlers = event_handlers

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
