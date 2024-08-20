import datetime as dt
import json
import uuid
from typing import Annotated, Optional
from fastapi import APIRouter, Depends, HTTPException, Response, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, TypeAdapter
from .common import get_bus, BasePagination
from src.infrastructure.message_bus import MessageBus
from src.domain import commands
from src.domain.model import Location as DLocation, State


router = APIRouter(prefix="/locations")


class ResidualShort(BaseModel):
    malo: str


class LocationSettings(BaseModel):
    active_from: dt.datetime
    active_until: Optional[dt.datetime] = None


class Location(BaseModel):
    state: str
    alias: Optional[str] = None
    id: Optional[str] = None
    residual_short: ResidualShort
    settings: LocationSettings


@router.get("/")
def get_locations(bus: Annotated[MessageBus, Depends(get_bus)]):
    with bus.uow as uow:
        locations: list[DLocation] = uow.locations.get_all()
        type_adapter = TypeAdapter(list[Location])

        locations = [
            Location(
                id=str(loc.id),
                state=loc.state,
                alias=loc.alias,
                residual_short=ResidualShort(malo=loc.residual_short.malo),
                settings=LocationSettings(
                    active_from=loc.settings.active_from,
                    active_until=loc.settings.active_until
                    if loc.settings.active_until
                    else None,
                ),
            )
            for loc in locations
        ]

        return BasePagination[Location](
            items=type_adapter.validate_python(locations), total=len(locations)
        )


@router.get("/{location_id}")
def get_location(bus: Annotated[MessageBus, Depends(get_bus)], location_id: str):
    with bus.uow as uow:
        location = uow.locations.get(uuid.UUID(location_id))
        if not location:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        return Location.model_validate(
            Location(
                id=str(location.id),
                state=location.state,
                alias=location.alias,
                residual_short=ResidualShort(malo=location.residual_short.malo),
                settings=LocationSettings(
                    active_from=location.settings.active_from,
                    active_until=location.settings.active_until
                    if location.settings.active_until
                    else None,
                ),
            )
        )


@router.post("/")
def add_location(bus: Annotated[MessageBus, Depends(get_bus)], fa_location: Location):
    state = State(fa_location.state)  # TODO primitives?
    residual_short = ResidualShort(malo=fa_location.residual_short.malo)
    location: DLocation = bus.handle(
        commands.CreateLocation(
            state=state,
            alias=fa_location.alias,
            residual_short_malo=residual_short.malo,
            settings_active_from=fa_location.settings.active_from,
            settings_active_until=fa_location.settings.active_until
            if fa_location.settings.active_until
            else None,
        )
    )
    return Location.model_validate(
        Location(
            id=str(location.id),
            state=location.state,
            alias=location.alias,
            residual_short=ResidualShort(malo=location.residual_short.malo),
            settings=LocationSettings(
                active_from=fa_location.settings.active_from,
                active_until=fa_location.settings.active_until,
            ),
        )
    )


@router.put("/{location_id}/settings")
def update_location_settings(
    bus: Annotated[MessageBus, Depends(get_bus)],
    location_id: str,
    fa_location_settings: LocationSettings,
):
    with bus.uow as uow:
        if not uow.locations.get(uuid.UUID(location_id)):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
        new_location: DLocation = bus.handle(
            commands.UpdateLocationSettings(
                location_id=location_id,
                settings_active_from=fa_location_settings.active_from,
                settings_active_until=fa_location_settings.active_until,
            )
        )
        return Location.model_validate(
            Location(
                id=str(new_location.id),
                state=new_location.state,
                alias=new_location.alias,
                residual_short=ResidualShort(malo=new_location.residual_short.malo),
                settings=LocationSettings(
                    active_from=new_location.settings.active_from,
                    active_until=new_location.settings.active_until,
                ),
            )
        )


@router.post("/{location_id}/update_location_data")
def update_location_historic_data(
    bus: Annotated[MessageBus, Depends(get_bus)], location_id: str
):
    bus.handle(commands.UpdateHistoricData(location_id=location_id))
    return Response(status_code=status.HTTP_202_ACCEPTED)


@router.post("/{location_id}/calculate_predictions")
def calculate_location_predictions(
    bus: Annotated[MessageBus, Depends(get_bus)], location_id: str
):
    bus.handle(commands.CalculatePredictions(location_id=location_id))
    return Response(status_code=status.HTTP_202_ACCEPTED)


@router.post("/{location_id}/send_predictions")
def send_predictions(bus: Annotated[MessageBus, Depends(get_bus)], location_id: str):
    bus.handle(commands.SendPredictions(location_id=location_id))
    return Response(status_code=status.HTTP_202_ACCEPTED)


@router.get("/{location_id}/predictions")
def list_location_predictions(
    bus: Annotated[MessageBus, Depends(get_bus)],
    location_id: str,
    type: str | None = None,
):
    prediction_response_body = []
    with bus.uow as uow:
        location: DLocation = uow.locations.get(id=uuid.UUID(location_id))
        if location:
            for prediction in location.predictions:
                if not type or (type and prediction.type == type):
                    prediction_response_body.append(
                        {
                            "type": prediction.type,
                            "df": json.loads(
                                prediction.df.to_json(
                                    orient="index", date_format="iso", date_unit="s"
                                )
                            ),
                        }
                    )
    return JSONResponse(prediction_response_body)


@router.post("/send_updated_predictions")
def send_updated_predictions_for_all(bus: Annotated[MessageBus, Depends(get_bus)]):
    bus.handle(commands.UpdatePredictAll())
    return Response(status_code=status.HTTP_202_ACCEPTED)
