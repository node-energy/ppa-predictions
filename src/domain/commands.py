import uuid
from datetime import datetime, date
from dataclasses import dataclass
from typing import Literal, Optional
from src.enums import DataRetriever, State, TransmissionSystemOperator


class Command:
    pass


@dataclass
class UpdatePredictAll(Command):
    pass


@dataclass
class UpdateHistoricData(Command):
    location_id: str


@dataclass
class CalculatePredictions(Command):
    location_id: str


@dataclass
class SendPredictions(Command):
    location_id: str


@dataclass
class CreateLocation(Command):
    id: Optional[uuid.UUID]
    state: State
    alias: Optional[str]
    tso: TransmissionSystemOperator
    residual_short: dict[str, uuid.UUID | str]
    residual_long: Optional[dict[str, uuid.UUID | str]]
    producers: Optional[list[dict[str, str | DataRetriever | uuid.UUID]]]
    settings_active_from: date
    settings_active_until: Optional[date]
    settings_send_consumption_predictions_to_fahrplanmanagement: bool
    settings_historic_days_for_consumption_prediction: int


@dataclass
class UpdateLocationSettings(Command):
    location_id: str
    settings_active_from: date
    settings_active_until: Optional[date]
    settings_send_consumption_predictions_to_fahrplanmanagement: bool
    settings_historic_days_for_consumption_prediction: int


@dataclass
class GetComponents(Command):
    pass


@dataclass
class CreateComponent(Command):
    location_ref: str
    malo: str
    type: Literal["producer", "consumer"]


@dataclass
class FetchLoadData(Command):
    pass


@dataclass
class MakePrediction(Command):
    use_data_from: datetime
    use_data_to: datetime
    predict_days: int
    component_ref: str
    predictor: Optional[str]


@dataclass
class FetchAllHistoricData(Command):
    pass


@dataclass
class FetchHistoricDataForComponent(Command):
    component_id: str
    historic_days: Optional[int] = 90


@dataclass
class MakeAllPredictions(Command):
    pass


@dataclass
class SendAllEigenverbrauchsPredictionsToImpuls(Command):
    send_even_if_not_sent_to_internal_fahrplanmanagement: Optional[bool] = False


@dataclass
class SendAllResidualLongPredictionsToImpuls(Command):
    send_even_if_not_sent_to_internal_fahrplanmanagement: Optional[bool] = False
