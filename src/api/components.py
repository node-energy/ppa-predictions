from fastapi import APIRouter, Depends, Response, status
from src.infrastructure.message_bus import MessageBus
from src.domain import commands


router = APIRouter(prefix="/components")


async def get_bus():
    bus = MessageBus()
    try:
        yield bus
    finally:
        pass


@router.get("/")
async def get_components(bus: MessageBus = Depends(get_bus)):
    components = bus.handle(commands.GetComponents())
    return components


@router.post("/")
async def add_component(bus: MessageBus = Depends(get_bus)):
    component = bus.handle(commands.CreateComponent(type='producer'))
    return {"message": f"Component ID: {component.ref}"}


@router.post("/{component_id}/load_profile")
async def add_historic_load_profile(component_id: str, bus: MessageBus = Depends(get_bus)):
    bus.handle(commands.AddHistoricLoadProfile(component_ref=component_id, timestamps=[]))
    return Response(status_code=status.HTTP_201_CREATED)
