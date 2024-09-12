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
from src.domain.model import Location as DLocation
from src.enums import DataRetriever, State, TransmissionSystemOperator

router = APIRouter(prefix="/locations")


class MarketLocation(BaseModel):
    id: Optional[uuid.UUID] = None
    number: str


class Producer(BaseModel):
    id: Optional[uuid.UUID] = None
    name: str
    market_location: MarketLocation
    prognosis_data_retriever: DataRetriever


class LocationSettings(BaseModel):
    active_from: dt.date
    active_until: Optional[dt.date] = None


class Location(BaseModel):
    id: Optional[uuid.UUID] = None
    state: str
    alias: Optional[str] = None
    tso: TransmissionSystemOperator
    residual_short: MarketLocation
    residual_long: Optional[MarketLocation] = None
    producers: Optional[list[Producer]] = []
    settings: LocationSettings

    @classmethod
    def from_domain(cls, location: DLocation):
        return cls(
            id=location.id,
            state=location.state,
            alias=location.alias,
            tso=location.tso.value,
            residual_short=MarketLocation(id=location.residual_short.id, number=location.residual_short.number),
            residual_long=MarketLocation(id=location.residual_long.id, number=location.residual_long.number) if location.residual_long else None,
            producers=[
                Producer(
                    id=p.id,
                    name=p.name,
                    market_location=MarketLocation(id=p.market_location.id, number=p.market_location.number),
                    prognosis_data_retriever=DataRetriever(p.prognosis_data_retriever)
                ) for p in location.producers
            ],
            settings=LocationSettings(
                active_from=location.settings.active_from,
                active_until=location.settings.active_until
                if location.settings.active_until
                else None,
            ),
        )


class SendPredictionToImpulsConfig(BaseModel):
    send_even_if_not_sent_to_internal_fahrplanmanagement: Optional[bool] = None


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
    location: DLocation = bus.handle(
        commands.CreateLocation(
            id=fa_location.id,
            state=state,
            alias=fa_location.alias,
            tso=fa_location.tso,
            residual_short={
                "id": fa_location.residual_short.id,
                "number": fa_location.residual_short.number,
            },
            residual_long={
                "id": fa_location.residual_long.id,
                "number": fa_location.residual_long.number,
            } if fa_location.residual_long else None,
            producers=[{
                "id": producer.id,
                "name": producer.name,
                "market_location_id": producer.market_location.id,
                "market_location_number": producer.market_location.number,
                "prognosis_data_retriever": producer.prognosis_data_retriever
            } for producer in fa_location.producers],   # TODO other datatype e.g. namedtuple possible here?
            settings_active_from=fa_location.settings.active_from,
            settings_active_until=fa_location.settings.active_until
            if fa_location.settings.active_until
            else None,
        )
    )
    return Location.model_validate(
        Location.from_domain(location)
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
                tso=new_location.tso,
                residual_short=MarketLocation(number=new_location.residual_short.number),
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
    with bus.uow as uow:
        if not uow.locations.get(uuid.UUID(location_id)):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    bus.handle(commands.UpdateHistoricData(location_id=location_id))
    return Response(status_code=status.HTTP_202_ACCEPTED)


@router.post("/{location_id}/calculate_predictions")
def calculate_location_predictions(
    bus: Annotated[MessageBus, Depends(get_bus)], location_id: str
):
    with bus.uow as uow:
        if not uow.locations.get(uuid.UUID(location_id)):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
    bus.handle(commands.CalculatePredictions(location_id=location_id))
    return Response(status_code=status.HTTP_202_ACCEPTED)


@router.post("/{location_id}/send_predictions")
def send_predictions(bus: Annotated[MessageBus, Depends(get_bus)], location_id: str):
    with bus.uow as uow:
        if not uow.locations.get(uuid.UUID(location_id)):
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
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
        if not location:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND)
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


@router.post("/send_eigenverbrauchs_predictions_impuls")
def send_all_eigenverbrauch_predictions_to_impuls_energy_trading(
    bus: Annotated[MessageBus, Depends(get_bus)],
    fa_send_prediction_to_impuls_config: SendPredictionToImpulsConfig
):
    bus.handle(commands.SendAllEigenverbrauchsPredictionsToImpuls(
        fa_send_prediction_to_impuls_config.send_even_if_not_sent_to_internal_fahrplanmanagement
    ))
    return Response(status_code=status.HTTP_202_ACCEPTED)


@router.post("/send_residual_long_predictions_impuls")
def send_all_residual_long_predictions_to_impuls_energy_trading(
    bus: Annotated[MessageBus, Depends(get_bus)],
    fa_send_prediction_to_impuls_config: SendPredictionToImpulsConfig
):
    bus.handle(commands.SendAllResidualLongPredictionsToImpuls(
        fa_send_prediction_to_impuls_config.send_even_if_not_sent_to_internal_fahrplanmanagement
    ))
    return Response(status_code=status.HTTP_202_ACCEPTED)
