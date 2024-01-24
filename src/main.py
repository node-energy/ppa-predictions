import inspect
from fastapi import FastAPI
from src.infrastructure.message_bus import MessageBus
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
    bus = MessageBus()
    bus.setup(MemoryUnitOfWork())


@app.get("/")
async def root():
    return {"message": "root"}
