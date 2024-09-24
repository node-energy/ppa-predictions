from pandas import DatetimeTZDtype
from pandera import Field, DataFrameModel, check
from pandera.typing import Index, Series, DataFrame
import datetime

from src.utils.market_location_number_validator import MarketLocationNumberValidator
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


class FahrplanmanagementSchema(DataFrameModel):
    datetime: Index[DatetimeTZDtype(tz=TIMEZONE_BERLIN)] = Field(
        ge=datetime.datetime(2020, 1, 1, tzinfo=TIMEZONE_BERLIN),
        alias="Timestamp (Europe/Berlin)",
        nullable=False,
        unique=True
    )
    value: Series = Field(
        ge=0,
        nullable=False,
        regex=True,
        alias=".*",
    )

    @check("^(?!Timestamp (Europe/Berlin))$", regex=True)
    def malo_number_header(cls, column: Series):
        MarketLocationNumberValidator()(column.name)
        return True

    @classmethod
    def from_time_series_schema(cls, df: DataFrame[TimeSeriesSchema], malo_number: str) -> DataFrame["FahrplanmanagementSchema"]:
        df = df.rename(columns={"value": malo_number})
        df.index.rename("Timestamp (Europe/Berlin)", inplace=True)
        return DataFrame[FahrplanmanagementSchema](df)

    class Config:
        strict = True   # allow no other columns than the ones specified in the schema


class IetLoadDataSchema(DataFrameModel):
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

