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


class ResidualLong(BaseModel):
    malo: str


class Producer(BaseModel):
    malo: str


class Location(BaseModel):
    state: str
    alias: Optional[str] = None
    id: Optional[str] = None
    residual_short: ResidualShort
    residual_long: Optional[ResidualLong] = None
    producers: Optional[list[Producer]] = []

    @classmethod
    def from_domain(cls, location: DLocation):
        return cls(
            state=location.state,
            alias=location.alias,
            id=str(location.id),
            residual_short=ResidualShort(malo=location.residual_short.malo),
            residual_long=ResidualLong(malo=location.residual_long.malo) if location.residual_long else None,
            producers=[Producer(malo=p.malo) for p in location.producers]

        )


@router.get("/")
def get_locations(bus: Annotated[MessageBus, Depends(get_bus)]):
    with bus.uow as uow:
        locations: list[DLocation] = uow.locations.get_all()
        type_adapter = TypeAdapter(list[Location])

        locations = [
            Location.from_domain(loc)
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
            Location.from_domain(location)
        )


@router.post("/")
def add_location(bus: Annotated[MessageBus, Depends(get_bus)], fa_location: Location):
    state = State(fa_location.state)  # TODO primitives?
    residual_short = ResidualShort(malo=fa_location.residual_short.malo)
    residual_long = ResidualLong(malo=fa_location.residual_long.malo) if fa_location.residual_long else None
    location: DLocation = bus.handle(
        commands.CreateLocation(
            state=state,
            alias=fa_location.alias,
            residual_short_malo=residual_short.malo,
            residual_long_malo=residual_long.malo,
            producer_malos=[producer.malo for producer in fa_location.producers]
        )
    )
    return Location.model_validate(
        Location.from_domain(location)
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
