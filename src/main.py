import logging
import sys
from fastapi import FastAPI
from datetime import datetime
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import MemoryUnitOfWork
from src.services.load_data import APILoadDataRetriever
from src.services.data_store import LocalDataStore
from src.api import components as components_api
from src.utils.decorators import repeat_at
from src.domain import commands


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler(sys.stdout)
log_formatter = logging.Formatter("%(asctime)s [%(processName)s: %(process)d] [%(threadName)s: %(thread)d] [%(levelname)s] %(name)s: %(message)s")
stream_handler.setFormatter(log_formatter)
logger.addHandler(stream_handler)


app = FastAPI(debug=True)
app.include_router(components_api.router)


@app.on_event("startup")
async def init_bus():
    bus = MessageBus()
    bus.setup(
        uow=MemoryUnitOfWork(),
        ldr=APILoadDataRetriever(),
        dst=LocalDataStore(),
    )


@app.get("/")
async def root():
    return {"message": "root"}


@app.on_event("startup")  # TODO replace with APScheduler
@repeat_at("20 16 * * *", logger=logger)
async def fetch_energy_data():
    bus = MessageBus()
    bus.handle(commands.FetchLoadData())
