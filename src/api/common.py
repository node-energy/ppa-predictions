from src.infrastructure.message_bus import MessageBus


async def get_bus():
    bus = MessageBus()
    try:
        yield bus
    finally:
        pass
