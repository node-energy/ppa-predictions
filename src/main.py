from fastapi import FastAPI
from src.infrastructure.message_bus import MessageBus
from src.infrastructure.unit_of_work import MemoryUnitOfWork
from src.api import components as components_api


app = FastAPI()
app.include_router(components_api.router)


@app.on_event("startup")
async def init_bus():
    bus = MessageBus()
    bus.setup(MemoryUnitOfWork())


@app.get("/")
async def root():
    return {"message": "root"}
