from uuid import UUID
from dataclasses import dataclass


class Event:
    pass


@dataclass
class CustomerCreated(Event):
    customer_id: UUID


@dataclass
class LocationCreated(Event):
    location_ref: str


@dataclass
class ProjectCreated(Event):
    project_ref: str
    name: str


@dataclass
class HistoricLoadProfileReceived(Event):
    component_ref: UUID


@dataclass
class PredictionsCreated(Event):
    location_id: str


@dataclass
class PredictionAdded(Event):
    location_id: str
