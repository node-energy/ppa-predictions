from __future__ import annotations

import abc
import uuid
import pandas as pd
from datetime import datetime
from enum import Enum
from typing import Optional
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
    alias: Optional[str] = None
    producers: list[Producer] = field(default_factory=list)
    residual_long: Optional[Producer] = None
    residual_short: Consumer
    predictions: list[Prediction] = field(default_factory=list)

    @property
    def has_production(self):
        return self.producers and len(self.producers) > 0

    def get_most_recent_prediction(self, prediction_type):
        return next(
            (p for p in sorted(self.predictions) if p.type == prediction_type), None
        )

    def calculate_local_consumption(self):
        result = None
        if not self.has_production:
            if not self.residual_short.historic_load_data:
                return None
                # self.events.append(events.MissingData())
            return self.residual_short.historic_load_data.df
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
        total_consumption_df = next(
            (p.df for p in self.predictions if p.type == PredictionType.CONSUMPTION),
            None,
        )
        total_production_df = next(
            (p.df for p in self.predictions if p.type == PredictionType.PRODUCTION),
            None,
        )

        short_prediction_df = (
            total_consumption_df - total_production_df
            if self.has_production
            else total_consumption_df
        )
        short_prediction_df[short_prediction_df < 0] = 0
        self.predictions.append(
            Prediction(df=short_prediction_df, type=PredictionType.RESIDUAL_SHORT)
        )

        if self.has_production:
            long_prediction_df = total_production_df - total_consumption_df
            long_prediction_df[long_prediction_df < 0] = 0
            self.predictions.append(
                Prediction(df=long_prediction_df, type=PredictionType.RESIDUAL_LONG)
            )
        # self.events.append(events.PredictionsCreated(location_id=str(self.id)))  # leads to send out predictions

    def add_prediction(self, prediction: Prediction):
        self.predictions.append(prediction)
        # self.events.append(events.PredictionAdded(location_id=str(self.id)))

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
    CONSUMPTION = "consumption"
    PRODUCTION = "production"
    RESIDUAL_SHORT = "short"
    RESIDUAL_LONG = "long"


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
