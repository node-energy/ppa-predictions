from __future__ import annotations
from uuid import UUID
from datetime import datetime, timezone
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase
from sqlalchemy import DateTime, ForeignKey, PickleType, UniqueConstraint
from typing import Optional


class Base(DeclarativeBase):
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
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
    alias: Mapped[Optional[str]]
    residual_long: Mapped[Optional[Component]] = relationship(
        back_populates="residual_long_location",
        foreign_keys="Component.residual_long_location_id",
    )
    residual_short: Mapped[Component] = relationship(
        back_populates="residual_short_location",
        foreign_keys="Component.residual_short_location_id",
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
    malo: Mapped[str]
    residual_short_location_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("locations.id")
    )
    residual_short_location: Mapped[Optional[Location]] = relationship(
        back_populates="residual_short", foreign_keys=[residual_short_location_id]
    )
    residual_long_location_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("locations.id")
    )
    residual_long_location: Mapped[Optional[Location]] = relationship(
        back_populates="residual_long", foreign_keys=[residual_long_location_id]
    )
    producer_location_id: Mapped[Optional[UUID]] = mapped_column(
        ForeignKey("locations.id")
    )
    producer_location: Mapped[Optional[Location]] = relationship(
        back_populates="producers", foreign_keys=[producer_location_id]
    )
    historic_load_data: Mapped[Optional[HistoricLoadData]] = relationship(
        back_populates="component",
        foreign_keys="HistoricLoadData.component_id",
        cascade="all, delete-orphan",
    )


class Prediction(Base, UUIDMixin):
    __tablename__ = "predictions"

    type: Mapped[str]
    dataframe: Mapped[bytes] = mapped_column(PickleType())
    location_id: Mapped[UUID] = mapped_column(ForeignKey("locations.id"))
    location: Mapped[Location] = relationship(
        back_populates="predictions", foreign_keys=[location_id]
    )


class HistoricLoadData(Base, UUIDMixin):
    __tablename__ = "historicloaddata"

    dataframe: Mapped[bytes] = mapped_column(PickleType())
    component_id: Mapped[UUID] = mapped_column(ForeignKey("components.id"))
    component: Mapped[Component] = relationship(
        back_populates="historic_load_data", foreign_keys=[component_id]
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
    active_from: Mapped[datetime] = mapped_column(DateTime)
    active_until: Mapped[Optional[datetime]] = mapped_column(DateTime)
