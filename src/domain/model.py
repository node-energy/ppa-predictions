from __future__ import annotations

import abc
import io
import uuid
import pandas as pd
from datetime import datetime
from enum import Enum
from uuid import UUID
from typing import List, Literal, Optional
from dataclasses import dataclass, field


@dataclass
class Entity:
    id: uuid = field(hash=True, default_factory=uuid.uuid4)

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __post_init__(cls):
        cls.__hash__ = Entity.__hash__


@dataclass(kw_only=True)
class AggregateRoot(Entity):
    events: list = field(default_factory=list)

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __post_init__(cls):
        cls.__hash__ = AggregateRoot.__hash__


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


@dataclass(kw_only=True)
class Location(AggregateRoot):
    __hash__ = AggregateRoot.__hash__
    state: State
    producers: list[Producer] = field(default_factory=list)
    residual_long: Optional[Producer] = None
    residual_short: Consumer
    predictions: list[Prediction] = field(default_factory=list)

    @property
    def has_production(self):
        return self.producers and len(self.producers) > 0

    @property
    def most_recent_prediction(self):
        return []

    def calculate_local_consumption(self):
        if not self.has_production:
            return (
                self.residual_short.historic_load_data.df  # TODO exception if no historic data is available
            )  # no production, we just use location "Bezug".
        # else: add all producer historic data, substract "Einspeisung" and add "Bezug"
        else:
            if not self.residual_long:
                return (
                    self.producers[0].historic_load_data.df
                    + self.residual_short.historic_load_data.df
                )
        return (
            self.producers[0].historic_load_data.df
            - self.residual_long.historic_load_data.df
            + self.residual_short.historic_load_data.df
        )  # TODO currently we only allow one producer per location

    def calculate_location_residual_loads(self):
        total_consumption_df = next((p.df for p in self.predictions if p.type == PredictionType.CONSUMPTION), None)
        total_production_df = next((p.df for p in self.predictions if p.type == PredictionType.PRODUCTION), None)

        short_prediction_df = total_consumption_df - total_production_df if self.has_production else total_consumption_df
        short_prediction_df[short_prediction_df < 0] = 0
        self.predictions.append(Prediction(df=short_prediction_df, type=PredictionType.RESIDUAL_SHORT))

        if self.has_production:
            long_prediction_df = total_production_df - total_consumption_df
            long_prediction_df[long_prediction_df < 0] = 0
            self.predictions.append(Prediction(df=long_prediction_df, type=PredictionType.RESIDUAL_LONG))

    def add_component(
        self, component: Component
    ):  # TODO fails silently, most likely not needed at all
        if isinstance(component, Consumer):
            if not self.consumers:
                self.consumers.append(component)
        if isinstance(component, Producer):
            if not self.producers:
                self.producers.append(component)


@dataclass(kw_only=True)
class Component(abc.ABC):
    malo: str
    name: Optional[str] = None
    historic_load_data: Optional[HistoricLoadData] = None


@dataclass(kw_only=True)
class Producer(Component, Entity):
    __hash__ = Entity.__hash__
    pass


@dataclass(kw_only=True)
class Consumer(Component, Entity):
    __hash__ = Entity.__hash__
    pass


@dataclass(kw_only=True)
class HistoricLoadData(Entity):
    __hash__ = Entity.__hash__
    updated: datetime = field(default_factory=datetime.now)
    df: pd.DataFrame

    def __eq__(self, other):
        return self.id == other.id

    def __gt__(self, other: HistoricLoadData):
        return self.updated > other.updated


class PredictionType(str, Enum):
    CONSUMPTION = 'consumption'
    PRODUCTION = 'production'
    RESIDUAL_SHORT = 'short'
    RESIDUAL_LONG = 'long'


@dataclass(kw_only=True)
class Prediction(Entity):
    __hash__ = Entity.__hash__
    updated: datetime = field(default_factory=datetime.now)
    df: pd.DataFrame
    type: PredictionType

    def __eq__(self, other):
        return self.id == other.id

    def __gt__(self, other: HistoricLoadData):
        return self.updated > other.updated


# value_object
@dataclass
class PredictionSettings:
    location: Location
