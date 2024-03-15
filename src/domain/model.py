from __future__ import annotations

import io
from datetime import datetime
from enum import Enum
from uuid import UUID
from typing import List, Literal, Optional
from dataclasses import dataclass

import pandas


@dataclass
class Project:
    id: UUID
    name: str

    def __eq__(self, other):
        if not isinstance(other, Project):
            return False
        return other.id == self.id

    def __hash__(self):
        return hash(self.id)


@dataclass
class Company:
    id: UUID
    name: str


@dataclass
class Customer:
    id: UUID


# Sharpen Counter, Component and Prediction entities, e.g. a prediction does not always belong to one component,
# but rather to a location
# value object
@dataclass
class State(str, Enum):
    baden_wurttemberg = "BW"
    bayern = "BY"
    berlin = "BE"
    brandenburg = "BB"
    bremen = "HB"
    hamburg = "HH"
    hessen = "HE"
    mecklenburg_vorpommern = "MV"
    niedersachsen = "NI"
    nordrhein_westfalen = "NW"
    rheinland_pfalz = "RP"
    saarland = "SL"
    sachsen = "SN"
    sachsen_anhalt = "ST"
    schleswig_holstein = "SH"
    thuringen = "TH"


@dataclass
class Location:
    id: UUID
    state: State
    company: Company
    project: Project

    def __eq__(self, other):
        if not isinstance(other, Location):
            return False
        return other.id == self.id

    def __hash__(self):
        return hash(self.id)


@dataclass(unsafe_hash=True)
class Component:
    id: UUID
    malo: str
    type: Literal["producer", "consumer", "scope"]
    location: Location
    name: Optional[str] = None


@dataclass()
class HistoricLoadProfile:
    id: UUID
    component: Component
    timestamps: List[TimeStamp]

    def __eq__(self, other):
        if not isinstance(other, HistoricLoadProfile):
            return False
        return other.id == self.id

    def __hash__(self):
        return hash(self.id)

    @classmethod
    def from_dataframe(cls, id, component, df):
        timestamps: List[TimeStamp] = []
        for index, row in df.iterrows():
            timestamps.append(
                TimeStamp(datetime=index.to_pydatetime(), value=row.iloc[0])
            )
        return cls(id, component, timestamps)

    def to_dataframe(self):
        return pandas.DataFrame(t.as_dict() for t in self.timestamps)


@dataclass
class Prediction:
    id: UUID
    component: Component
    created: datetime
    timestamps: List[TimeStamp]

    @classmethod
    def from_dataframe(cls, id, component, created, df):
        timestamps: List[TimeStamp] = []
        for index, row in df.iterrows():
            timestamps.append(
                TimeStamp(datetime=index.to_pydatetime(), value=row.iloc[0])
            )
        return cls(id, component, created, timestamps)

    def to_dataframe(self):
        return pandas.DataFrame(t.as_dict() for t in self.timestamps)

    def to_csv_buffer(self):
        df = self.to_dataframe()
        df.rename(
            columns={
                "datetime": "Timestamp (Europe/Berlin)",
                "value": self.component.malo,
            },
            inplace=True,
        )

        csv_buffer = io.BytesIO()
        return df.to_csv(csv_buffer, sep=";", index=False)


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

    def as_dict(self):
        return {"datetime": self.datetime, "value": self.value}


# value_object
@dataclass
class PredictionSettings:
    location: Location
