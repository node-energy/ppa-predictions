from __future__ import annotations


from uuid import UUID
from datetime import datetime, timezone
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Mapped, mapped_column, relationship, DeclarativeBase
from sqlalchemy import Column, DateTime, String, ForeignKey, PickleType
from typing import Optional


# Base = declarative_base()
# TODO  audit fields created_at, updated_at


class UUIDBase(DeclarativeBase):
    id: Mapped[UUID] = mapped_column(primary_key=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime, default=lambda: datetime.now(timezone.utc)
    )


# class Project(UUIDBase):
#     __tablename__ = "project"
#
#     name: Mapped[str]
#     locations: Mapped[list[Location]] = relationship(
#         back_populates="project", lazy="noload"
#     )
#
#
# class Company(UUIDBase):
#     __tablename__ = "company"
#     locations: Mapped[list[Location]] = relationship(
#         back_populates="company", lazy="noload"
#     )
#
#
# class Location_old(UUIDBase):
#     __tablename__ = "location_old"
#
#     state: Mapped[str]
#     project_id: Mapped[UUID] = mapped_column(ForeignKey("project.id"))
#     project: Mapped[Project] = relationship(
#         lazy="joined", innerjoin=True, viewonly=True
#     )
#     company_id: Mapped[UUID] = mapped_column(ForeignKey("company.id"))
#     company: Mapped[Company] = relationship(
#         lazy="joined", innerjoin=True, viewonly=True
#     )
#     components: Mapped[list[Component]] = relationship(
#         back_populates="location", lazy="noload"
#     )


class Location(UUIDBase):
    __tablename__ = "locations"

    state: Mapped[str]
    residual_long: Mapped[Optional[Component]] = relationship(back_populates="residual_long_location", foreign_keys="Component.residual_long_location_id")
    residual_short: Mapped[Component] = relationship(back_populates="residual_short_location", foreign_keys="Component.residual_short_location_id")
    producers: Mapped[list[Component]] = relationship(back_populates="producer_location", foreign_keys="Component.producer_location_id")
    predictions: Mapped[list[Prediction]] = relationship(back_populates="location", foreign_keys="Prediction.location_id")
    # predictions: Mapped[list[Prediction]] = relationship(
    #     back_populates="locations", lazy="noload"
    # )


class Component(UUIDBase):
    __tablename__ = "components"

    type: Mapped[str]
    malo: Mapped[str]
    residual_short_location_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("locations.id"))
    residual_short_location: Mapped[Optional[Location]] = relationship(back_populates="residual_short", foreign_keys=[residual_short_location_id])
    residual_long_location_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("locations.id"))
    residual_long_location: Mapped[Optional[Location]] = relationship(back_populates="residual_long", foreign_keys=[residual_long_location_id])
    producer_location_id: Mapped[Optional[UUID]] = mapped_column(ForeignKey("locations.id"))
    producer_location: Mapped[Optional[Location]] = relationship(back_populates="producers", foreign_keys=[producer_location_id])
    historic_load_data: Mapped[Optional[HistoricLoadData]] = relationship(back_populates="component", foreign_keys="HistoricLoadData.component_id")


class Prediction(UUIDBase):
    __tablename__ = "predictions"

    type: Mapped[str]
    dataframe: Mapped[bytes] = mapped_column(PickleType())
    location_id: Mapped[UUID] = mapped_column(ForeignKey("locations.id"))
    location: Mapped[Location] = relationship(back_populates="predictions", foreign_keys=[location_id])


class HistoricLoadData(UUIDBase):
    __tablename__ = "historicloaddata"

    dataframe: Mapped[bytes] = mapped_column(PickleType())
    component_id: Mapped[UUID] = mapped_column(ForeignKey("components.id"))
    component: Mapped[Component] = relationship(back_populates="historic_load_data", foreign_keys=[component_id])


# class Component_old(
#     UUIDBase
# ):  # TODO optional relationships to location or component (tree)
#     __tablename__ = "component_old"
#
#     type: Mapped[str]
#     location_id: Mapped[UUID] = mapped_column(ForeignKey("location.id"))
#     location: Mapped[Location] = relationship(
#         lazy="joined", innerjoin=True, viewonly=True
#     )
#     malo: Mapped[str]
#     name: Mapped[Optional[str]] = mapped_column(String(60))
#     predictions: Mapped[list[Prediction]] = relationship(
#         back_populates="component", lazy="noload"
#     )
#     historic_load_profiles: Mapped[list[HistoricLoadProfile]] = relationship(
#         back_populates="component", lazy="noload"
#     )
#
#
# class Prediction(UUIDBase):
#     __tablename__ = "prediction"
#
#     component_id: Mapped[UUID] = mapped_column(ForeignKey("component.id"))
#     component: Mapped[Component] = relationship(
#         lazy="joined", innerjoin=True, viewonly=True
#     )
#
#     # timestamps?
#
#
# class HistoricLoadProfile(UUIDBase):
#     __tablename__ = "historic_load_profile"
#
#     component_id: Mapped[UUID] = mapped_column(ForeignKey("component.id"))
#     component: Mapped[Component] = relationship(
#         lazy="joined", innerjoin=True, viewonly=True
#     )
#
#     # timestamps?
