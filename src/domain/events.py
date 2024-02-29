from uuid import UUID
from dataclasses import dataclass


class Event:
    pass


@dataclass
class CustomerCreated(Event):
    customer_id: UUID


@dataclass
class HistoricLoadProfileReceived(Event):
    component_ref: UUID


@dataclass
class PredictionCreated(Event):
    prediction_ref: UUID
