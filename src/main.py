from fastapi import FastAPI
from datetime import datetime
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import MemoryUnitOfWork
from src.api import components as components_api
from src.utils.decorators import repeat_at
from src.domain import commands


app = FastAPI()
app.include_router(components_api.router)


@app.on_event("startup")
async def init_bus():
    bus = MessageBus()
    bus.setup(MemoryUnitOfWork())


@app.get("/")
async def root():
    return {"message": "root"}


@repeat_at("0 8 * * *")
async def fetch_energy_data():
    bus = MessageBus()
    bus.handle(commands.FetchLoadData())
