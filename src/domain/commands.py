from datetime import datetime
from dataclasses import dataclass
from typing import List, Literal, Optional
from src.domain.model import TimeStamp


class Command:
    pass


@dataclass
class CreateCustomer(Command):
    pass


@dataclass
class GetComponents(Command):
    pass


@dataclass
class CreateComponent(Command):
    location_ref: str
    type: Literal['producer', 'consumer']


@dataclass
class AddHistoricLoadProfile(Command):
    component_ref: str
    timestamps: List[TimeStamp]


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
