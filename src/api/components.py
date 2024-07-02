from fastapi import APIRouter, Depends, Response, status
from pydantic import BaseModel, Field
from src.infrastructure.message_bus import MessageBus
from src.domain import commands


router = APIRouter(prefix="/components")


async def get_bus():
    bus = MessageBus()
    try:
        yield bus
    finally:
        pass


class Component(BaseModel):
    id: str | None
    type: str = Field(default="producer")
    location_ref: str = Field(default="1")
    malo: str


@router.get("/")
async def get_components(bus: MessageBus = Depends(get_bus)):
    components = bus.handle(commands.GetComponents())
    return components


@router.post("/")
async def add_component(fa_component: Component, bus: MessageBus = Depends(get_bus)):
    component = bus.handle(
        commands.CreateComponent(
            type="producer",
            location_ref=fa_component.location_ref,
            malo=fa_component.malo,
        )
    )
    return {"message": f"Component ID: {component.id}"}


@router.post("/{component_id}/load_profile")
async def add_historic_load_profile(
    component_id: str, bus: MessageBus = Depends(get_bus)
):
    bus.handle(
        commands.AddHistoricLoadProfile(component_ref=component_id, timestamps=[])
    )
    return Response(status_code=status.HTTP_201_CREATED)
