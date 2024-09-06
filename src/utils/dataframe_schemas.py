from pandas import DatetimeTZDtype
from pandera import Field, DataFrameModel, check
from pandera.typing import Index, Series
import datetime

from src.utils.timezone import TIMEZONE_BERLIN, TIMEZONE_UTC


class TimeSeriesSchema(DataFrameModel):
    datetime: Index[DatetimeTZDtype(tz=TIMEZONE_BERLIN)] = Field(
        ge=datetime.datetime(2020, 1, 1, tzinfo=TIMEZONE_BERLIN),
        nullable=False,
        unique=True
    )
    value: Series = Field(ge=0, nullable=True)

    class Config:
        strict = True   # allow no other columns than the ones specified in the schema


class IetEigenverbrauchSchema(DataFrameModel):
    datetime: Index[DatetimeTZDtype(tz=TIMEZONE_UTC)] = Field(
        alias="#timestamp",
        ge=datetime.datetime(2020, 1, 1, tzinfo=TIMEZONE_UTC),
        nullable=False,
        unique=True
    )
    value: Series = Field(
        ge=0,
        nullable=False,
        regex=True,
        alias=".*",
    )

    @check("^(?!#timestamp).*", regex=True)
    def max_three_digits(cls, column: Series):
        return column.round(3).equals(column)

    @check("^(?!#timestamp).*", regex=True)
    def data_probably_in_mw(cls, column: Series):
        # rudimentary check that the passed data is in unit MW
        # currently there is no site in our portfolio that has a max load of more than 10 MW
        return column.max() < 10


class IetResidualLoadSchema(DataFrameModel):
    class Config:
        strict = True   # allow no other columns than the ones specified in the schema

    datetime: Index[DatetimeTZDtype(tz=TIMEZONE_UTC)] = Field(
        alias="#timestamp",
        ge=datetime.datetime(2020, 1, 1, tzinfo=TIMEZONE_UTC),
        nullable=False,
        unique=True
    )
    transnetbw: Series = Field(
        ge=0,
        nullable=False,
        alias="TransnetBW",
    )
    tennet: Series = Field(
        ge=0,
        nullable=False,
        alias="TenneT",
    )
    amprion: Series = Field(
        ge=0,
        nullable=False,
        alias="Amprion",
    )
    hertz: Series = Field(
        ge=0,
        nullable=False,
        alias="50Hertz"
    )

    @check("^(?!#timestamp).*", regex=True)
    def max_three_digits(cls, column: Series):
        return column.round(3).equals(column)

    @check("^(?!#timestamp).*", regex=True)
    def data_probably_in_mw(cls, column: Series):
        # rudimentary check that the passed data is in unit MW
        # currently there is no site in our portfolio that has a max load of more than 10 MW
        return column.max() < 10
