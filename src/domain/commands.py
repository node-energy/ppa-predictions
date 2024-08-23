from datetime import datetime, date
from dataclasses import dataclass
from typing import Literal, Optional
from src.domain.model import State


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
    state: State
    alias: Optional[str]
    residual_short_malo: str
    residual_long_malo: Optional[str]
    producer_malos: Optional[list[str]]
    settings_active_from: date
    settings_active_until: Optional[date]


@dataclass
class UpdateLocationSettings(Command):
    location_id: str
    settings_active_from: datetime
    settings_active_until: Optional[datetime]


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
