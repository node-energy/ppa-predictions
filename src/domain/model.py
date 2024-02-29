from __future__ import annotations
from datetime import datetime
from enum import Enum
from uuid import UUID
from typing import List, Literal
from dataclasses import dataclass
from pandas import DataFrame, DatetimeIndex


@dataclass
class Customer:
    ref: UUID


# Sharpen Counter, Component and Prediction entities, e.g. a prediction does not always belong to one component,
# but rather to a location
# value object
@dataclass
class State(str, Enum):
    baden_wurttemberg = 'BW'
    bayern = 'BY'
    berlin = 'BE'
    brandenburg = 'BB'
    bremen = 'HB'
    hamburg = 'HH'
    hessen = 'HE'
    mecklenburg_vorpommern = 'MV'
    niedersachsen = 'NI'
    nordrhein_westfalen = 'NW'
    rheinland_pfalz = 'RP'
    saarland = 'SL'
    sachsen = 'SN'
    sachsen_anhalt = 'ST'
    schleswig_holstein = 'SH'
    thuringen = 'TH'


@dataclass
class Location:
    ref: UUID
    state: State
    customer: Customer


@dataclass(unsafe_hash=True)
class Component:
    ref: UUID
    malo: str
    type: Literal["producer", "consumer"]
    location: Location


@dataclass()
class HistoricLoadProfile:
    ref: UUID
    component: Component
    timestamps: List[TimeStamp]

    def __eq__(self, other):
        if not isinstance(other, HistoricLoadProfile):
            return False
        return other.ref == self.ref

    def __hash__(self):
        return hash(self.ref)

    @classmethod
    def from_dataframe(cls, ref, component, df):
        timestamps: List[TimeStamp] = []
        for index, row in df.iterrows():
            timestamps.append(
                TimeStamp(datetime=index.to_pydatetime(), value=row.iloc[0])
            )
        return cls(ref, component, timestamps)


@dataclass
class Prediction:
    ref: UUID
    component: Component
    created: datetime
    timestamps: List[TimeStamp]


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
    location: Location
