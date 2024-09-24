from __future__ import annotations

import abc
import logging
import uuid
import pandas as pd
from datetime import datetime, date, time
from typing import Optional
from dataclasses import dataclass, field

from pandera.typing import DataFrame

from src.enums import Measurand, DataRetriever, PredictionType, State, PredictionReceiver, TransmissionSystemOperator
from src.utils.dataframe_schemas import TimeSeriesSchema
from src.utils.timezone import utc_now


logger = logging.getLogger(__name__)


@dataclass
class Entity:
    id: uuid = field(hash=True, default_factory=uuid.uuid4)

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __post_init__(cls):
        cls.__hash__ = Entity.__hash__


@dataclass(frozen=True)
class ValueObject:
    pass


@dataclass(kw_only=True)
class AggregateRoot(Entity):
    events: list = field(default_factory=list)

    def __eq__(self, other):
        return self.id == other.id

    def __hash__(self):
        return hash(self.id)

    def __post_init__(cls):
        cls.__hash__ = AggregateRoot.__hash__


@dataclass(kw_only=True)
class Location(AggregateRoot):
    __hash__ = AggregateRoot.__hash__
    settings: LocationSettings
    state: State
    tso: TransmissionSystemOperator
    alias: Optional[str] = None
    producers: list[Producer] = field(default_factory=list)
    residual_long: Optional[MarketLocation] = None
    residual_short: MarketLocation
    predictions: list[Prediction] = field(default_factory=list)

    @property
    def has_production(self):
        return self.producers and len(self.producers) > 0

    def get_most_recent_prediction(
        self,
        prediction_type,
        receiver: Optional[PredictionReceiver]=None,
        sent_before: Optional[time] = None
    ) -> Optional[Prediction]:
        sorted_predictions = (p for p in sorted(self.predictions, reverse=True) if p.type == prediction_type)
        if not receiver and not sent_before:
            return next(sorted_predictions, None)

        for prediction in sorted_predictions:
            shipments = prediction.shipments
            if receiver:
                shipments = filter(lambda shipment: receiver == shipment.receiver, shipments)
            if sent_before:
                if sent_before.tzinfo is None:
                    raise ValueError("<sent_before> must have a timezone")
                shipments = filter(lambda shipment: shipment.created.astimezone(sent_before.tzinfo).time().replace(tzinfo=sent_before.tzinfo) < sent_before, shipments)
            if any(shipments):
                return prediction
        return None

    def calculate_local_consumption(self):
        if not self.has_production:
            if not self.residual_short.historic_load_data:
                return None
                # self.events.append(events.MissingData())
            return self.residual_short.historic_load_data.df
        else:
            if not self.residual_long:
                return (
                    self.producers[0].market_location.historic_load_data.df
                    + self.residual_short.historic_load_data.df
                )
        return (
            self.producers[0].market_location.historic_load_data.df
            - self.residual_long.historic_load_data.df
            + self.residual_short.historic_load_data.df
        )  # TODO currently we only allow one producer per location

    def calculate_location_residual_loads(self):
        # todo cut prognosis df, so that it starts at prognosis horizon (next day)
        total_consumption_df = self.get_most_recent_prediction(PredictionType.CONSUMPTION).df
        total_production = self.get_most_recent_prediction(PredictionType.PRODUCTION)
        if total_production:
            total_production_df = total_production.df

        def clip_to_time_range(df: pd.DataFrame) -> pd.DataFrame:
            return df[
                (df.index.date >= self.settings.active_from)
                & (
                    df.index.date < self.settings.active_until
                    if self.settings.active_until
                    else True
                )
            ]

        short_prediction_df = (
            total_consumption_df - total_production_df
            if self.has_production
            else total_consumption_df
        )
        short_prediction_df[short_prediction_df < 0] = 0
        short_prediction_df = clip_to_time_range(short_prediction_df)
        short_prediction_df = short_prediction_df[
            short_prediction_df.first_valid_index():short_prediction_df.last_valid_index()
        ]
        self.predictions.append(
            Prediction(df=short_prediction_df, type=PredictionType.RESIDUAL_SHORT)
        )

        if self.has_production:
            long_prediction_df = total_production_df - total_consumption_df
            long_prediction_df[long_prediction_df < 0] = 0
            long_prediction_df = clip_to_time_range(long_prediction_df)
            long_prediction_df = long_prediction_df[
                long_prediction_df.first_valid_index():long_prediction_df.last_valid_index()
            ]
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

    def delete_oldest_predictions(self, keep: int = 3, type: PredictionType = None):
        predictions = self.predictions

        if type:
            predictions = [p for p in self.predictions if p.type == type]

        predictions_to_remove = sorted(predictions, reverse=True)[keep:]
        self.predictions = [
            p for p in self.predictions if p not in predictions_to_remove
        ]

    def get_predicted_own_consumption(
        self,
        mandatory_previous_receivers: Optional[PredictionReceiver] = None,
        sent_before: Optional[time] = None
    ):
        if not self.has_production:
            logger.warning("Location has no production, cannot calculate own consumption")
            return None
        consumption_prediction = self.get_most_recent_prediction(
            prediction_type=PredictionType.CONSUMPTION,
            receiver=mandatory_previous_receivers,
            sent_before=sent_before,
        )
        production_prediction = self.get_most_recent_prediction(
            prediction_type=PredictionType.PRODUCTION,
            receiver=mandatory_previous_receivers,
            sent_before=sent_before,
        )
        if consumption_prediction and production_prediction:
            df = consumption_prediction.df.clip(upper=production_prediction.df)
            return DataFrame[TimeSeriesSchema](df[df.first_valid_index(): df.last_valid_index()])
        return None




@dataclass(kw_only=True)
class MarketLocation(Entity):
    number: str
    measurand: Measurand
    historic_load_data: Optional[HistoricLoadData] = None


@dataclass(kw_only=True)
class Component(abc.ABC):
    market_location: MarketLocation
    name: Optional[str] = None


@dataclass(kw_only=True)
class Producer(Component, Entity):
    __hash__ = Entity.__hash__
    prognosis_data_retriever: DataRetriever


@dataclass(kw_only=True)
class Consumer(Component, Entity):
    __hash__ = Entity.__hash__
    pass


@dataclass(kw_only=True)
class HistoricLoadData(Entity):
    __hash__ = Entity.__hash__
    created: datetime = field(default_factory=utc_now)  # this default is only used for newly created predictions in memory, value will be overwritten with current datetime when saved to database
    df: pd.DataFrame

    def __eq__(self, other):
        return self.id == other.id

    def __gt__(self, other: HistoricLoadData):
        return self.created > other.created


@dataclass(kw_only=True)
class Prediction(Entity):
    __hash__ = Entity.__hash__
    created: datetime = field(default_factory=utc_now)  # this default is only used for newly created predictions in memory, value will be overwritten with current datetime when saved to database
    df: pd.DataFrame
    type: PredictionType
    shipments: list[PredictionShipment] = field(default_factory=list)

    def __eq__(self, other):
        return self.id == other.id

    def __gt__(self, other: Prediction):
        return self.created > other.created


@dataclass(kw_only=True)
class PredictionShipment(Entity):
    created: datetime = field(default_factory=utc_now)
    receiver: PredictionReceiver

    def __eq__(self, other):
        return self.id == other.id

    def __gt__(self, other: Prediction):
        return self.created > other.created


# value_object
@dataclass
class PredictionSettings:
    location: Location


@dataclass(kw_only=True, frozen=True)
class LocationSettings(ValueObject):
    active_from: date
    active_until: Optional[date]
