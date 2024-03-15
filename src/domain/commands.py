from datetime import datetime
from dataclasses import dataclass
from typing import List, Literal, Optional
from src.domain.model import TimeStamp, State


class Command:
    pass


@dataclass
class CreateCustomer(Command):
    pass


@dataclass
class CreateProject(Command):
    name: str


@dataclass
class CreateLocation(Command):
    project_ref: str
    state: State
    company_ref: str


@dataclass
class CreateScope(Command):
    location_ref: str


@dataclass
class GetComponents(Command):
    pass


@dataclass
class CreateComponent(Command):
    location_ref: str
    malo: str
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
