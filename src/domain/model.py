from __future__ import annotations
from datetime import datetime
from uuid import UUID
from typing import List, Literal, Set
from dataclasses import dataclass


@dataclass
class Customer:
    id: UUID

# Sharpen Counter, Component and Prediction entities, e.g. a prediction does not always belong to one component,
# but rather to a location


@dataclass(unsafe_hash=True)
class Component:
    ref: UUID
    type: Literal['producer', 'consumer']

    def add_historic_load_profile(self, timestamps: List[TimeStamp]):
        for timestamp in sorted(timestamps):
            print(timestamp)


@dataclass
class HistoricLoadProfile:
    ref: UUID
    component: Component
    timestamps: Set[TimeStamp]


@dataclass
class Prediction:
    ref: UUID
    component: Component
    created: datetime
    timestamps: Set[TimeStamp]


# value_object
@dataclass(unsafe_hash=True)
class TimeStamp:
    datetime: datetime
    value: float  # use int?

    def __gt__(self, other: TimeStamp):
        if self.datetime is None:
            return False
        if other.datetime is None:
            return True
        return self.datetime > other.datetime


# value_object
@dataclass
class PredictionSettings:
    location: str
