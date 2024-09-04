from pandas import DatetimeTZDtype
from pandas.core.dtypes.common import is_numeric_dtype
from pandera import Field, DataFrameModel, check
from pandera.typing import Index, Series
import datetime

from src.utils.timezone import TIMEZONE_BERLIN


class TimeSeriesSchema(DataFrameModel):
    datetime: Index[DatetimeTZDtype(tz=TIMEZONE_BERLIN)] = Field(
        ge=datetime.datetime(2020, 1, 1, tzinfo=TIMEZONE_BERLIN),
        nullable=False,
        unique=True
    )
    value: Series = Field(ge=0, nullable=True)

    @check("value")
    def check_is_number(cls, column_header: Series):
        return is_numeric_dtype(column_header)

    class Config:
        strict = True   # allow no other columns than the ones specified in the schema