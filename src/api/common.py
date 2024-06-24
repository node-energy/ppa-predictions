from pydantic import BaseModel
from src.infrastructure.message_bus import MessageBus


async def get_bus():
    bus = MessageBus()
    try:
        yield bus
    finally:
        pass


class BasePagination[T](BaseModel):
    items: list[T]
    total: int
