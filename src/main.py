import inspect
from fastapi import FastAPI
from src.infrastructure.message_bus import MessageBus
from src.domain.handlers import COMMAND_HANDLERS, EVENT_HANDLERS
from src.infrastructure.unit_of_work import MemoryUnitOfWork
from src.api import components as components_api


app = FastAPI()
app.include_router(components_api.router)


def inject_dependencies(handler, dependencies):
    params = inspect.signature(handler).parameters
    deps = {
        name: dependency
        for name, dependency in dependencies.items()
        if name in params
    }
    return lambda message: handler(message, **deps)


@app.on_event("startup")
async def init_bus():
    uow = MemoryUnitOfWork()
    dependencies = {"uow": uow}

    injected_command_handlers = {
        command_type: inject_dependencies(handler, dependencies)
        for command_type, handler in COMMAND_HANDLERS.items()
    }
    injected_event_handlers = {
        event_type: [
            inject_dependencies(handler, dependencies)
            for handler in event_handlers
        ]
        for event_type, event_handlers in EVENT_HANDLERS.items()
    }
    bus = MessageBus()
    bus.setup(
        uow,
        injected_command_handlers,
        injected_event_handlers
    )


@app.get("/")
async def root():
    return {"message": "root"}
