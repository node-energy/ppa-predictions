from uuid import UUID
from dataclasses import dataclass
from typing import List, Literal
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
    component_id: str
    timestamps: List[TimeStamp]
