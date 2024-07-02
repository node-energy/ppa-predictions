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


class Location(BaseModel):
    state: str
    id: Optional[str] = None
    residual_short: ResidualShort


@router.get("/")
def get_locations(bus: Annotated[MessageBus, Depends(get_bus)]):
    with bus.uow as uow:
        locations: list[DLocation] = uow.locations.get_all()
        type_adapter = TypeAdapter(list[Location])

        locations = [
            Location(
                id=str(loc.id),
                state=loc.state,
                residual_short=ResidualShort(malo=loc.residual_short.malo),
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
                residual_short=ResidualShort(malo=location.residual_short.malo),
            )
        )


@router.post("/")
def add_location(bus: Annotated[MessageBus, Depends(get_bus)], fa_location: Location):
    state = State(fa_location.state)  # TODO primitives?
    residual_short = ResidualShort(malo=fa_location.residual_short.malo)
    location: DLocation = bus.handle(
        commands.CreateLocation(state=state, residual_short_malo=residual_short.malo)
    )
    return Location.model_validate(
        Location(
            id=str(location.id),
            state=location.state,
            residual_short=ResidualShort(malo=location.residual_short.malo),
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
