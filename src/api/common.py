from typing import Generic, TypeVar

from pydantic import BaseModel
from src.infrastructure.message_bus import MessageBus


T = TypeVar("T")


async def get_bus():
    bus = MessageBus()
    try:
        yield bus
    finally:
        pass


class BasePagination(BaseModel, Generic[T]):
    items: list[T]
    total: int
