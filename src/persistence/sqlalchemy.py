from __future__ import annotations
from uuid import UUID
from datetime import datetime, date
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase
from sqlalchemy import DateTime, Date, ForeignKey, PickleType, UniqueConstraint
from typing import Optional

from src.utils.timezone import TIMEZONE_UTC


class Base(DeclarativeBase):
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(TIMEZONE_UTC)
    )


class UUIDMixin:
    id: Mapped[UUID] = mapped_column(primary_key=True)


class Location(Base, UUIDMixin):
    __tablename__ = "locations"

    settings: Mapped[LocationSettings] = relationship(
        back_populates="location",
        cascade="all, delete-orphan",
    )
    state: Mapped[str]
    tso: Mapped[str]
    alias: Mapped[Optional[str]]
    residual_long: Mapped[Optional[MarketLocation]] = relationship(
        back_populates="residual_long_location",
        foreign_keys="MarketLocation.residual_long_location_id",
    )
    residual_short: Mapped[MarketLocation] = relationship(
        back_populates="residual_short_location",
        foreign_keys="MarketLocation.residual_short_location_id",
    )
    producers: Mapped[list[Component]] = relationship(
        back_populates="producer_location",
        foreign_keys="Component.producer_location_id",
    )
    predictions: Mapped[list[Prediction]] = relationship(
        back_populates="location",
        foreign_keys="Prediction.location_id",
        cascade="all, delete-orphan",
    )


class Component(Base, UUIDMixin):
    __tablename__ = "components"

    type: Mapped[str]
    name: Mapped[str]
    market_location: Mapped[MarketLocation] = relationship(
        back_populates="component", foreign_keys="MarketLocation.component_id", cascade="all, delete",   # todo think about cascade behaviour
    )
    producer_location_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("locations.id")
    )
    producer_location: Mapped[Optional[Location]] = relationship(
        back_populates="producers", foreign_keys=[producer_location_id]
    )
    prognosis_data_retriever: Mapped[Optional[str]]


class MarketLocation(Base, UUIDMixin):
    __tablename__ = "marketlocations"

    number: Mapped[str]
    metering_direction: Mapped[str]
    historic_load_data: Mapped[Optional[HistoricLoadData]] = relationship(
        back_populates="market_location",
        foreign_keys="HistoricLoadData.market_location_id",
        cascade="all, delete-orphan",
    )
    component_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("components.id"))
    component: Mapped[Optional[Component]] = relationship(
        back_populates="market_location", foreign_keys=[component_id]
    )
    residual_long_location_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("locations.id")
    )
    residual_long_location: Mapped[Optional[Location]] = relationship(
        back_populates="residual_long", foreign_keys=[residual_long_location_id]
    )
    residual_short_location_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("locations.id")
    )
    residual_short_location: Mapped[Optional[Location]] = relationship(
        back_populates="residual_short", foreign_keys=[residual_short_location_id]
    )


class Prediction(Base, UUIDMixin):
    __tablename__ = "predictions"

    type: Mapped[str]
    dataframe: Mapped[bytes] = mapped_column(PickleType())
    location_id: Mapped[UUID] = mapped_column(ForeignKey("locations.id"))
    location: Mapped[Location] = relationship(
        back_populates="predictions", foreign_keys=[location_id]
    )
    shipments: Mapped[list[PredictionShipment]] = relationship(
        back_populates="prediction",
        foreign_keys="PredictionShipment.prediction_id",
        cascade="all, delete-orphan",
    )


class PredictionShipment(Base, UUIDMixin):
    __tablename__ = "predictionshipments"

    prediction_id: Mapped[UUID] = mapped_column(ForeignKey("predictions.id"))
    prediction: Mapped[Prediction] = relationship(
        back_populates="shipments", foreign_keys=[prediction_id]
    )
    receiver: Mapped[str]


class HistoricLoadData(Base, UUIDMixin):
    __tablename__ = "historicloaddata"

    dataframe: Mapped[bytes] = mapped_column(PickleType())
    market_location_id: Mapped[UUID] = mapped_column(ForeignKey("marketlocations.id"))
    market_location: Mapped[MarketLocation] = relationship(
        back_populates="historic_load_data", foreign_keys=[market_location_id]
    )


class LocationSettings(Base):
    __tablename__ = "locationsettings"
    __table_args__ = (UniqueConstraint("location_id"),)

    id: Mapped[int] = mapped_column(primary_key=True)
    location_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("locations.id"))
    location: Mapped[Optional[Location]] = relationship(
        back_populates="settings",
        foreign_keys=[location_id],
    )
    active_from: Mapped[date] = mapped_column(Date)
    active_until: Mapped[Optional[date]] = mapped_column(Date)
