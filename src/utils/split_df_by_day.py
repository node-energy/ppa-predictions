import datetime
import zoneinfo
from collections import OrderedDict

import pandas as pd


def split_df_by_day(df: pd.DataFrame, timezone_for_day_boundary: zoneinfo.ZoneInfo | None) -> OrderedDict[datetime.date, [pd.DataFrame]]:
    dfs_by_day = OrderedDict()
    if not timezone_for_day_boundary:
        timestamps_series = pd.Series(index=df.index)
    else:
        timestamps_series = pd.Series(df.tz_convert(timezone_for_day_boundary).index)
    unique_dates = timestamps_series.dt.date.unique()
    for day in unique_dates:
        dfs_by_day[day] = df[df.tz_convert(timezone_for_day_boundary).index.date == day]
    return dfs_by_day
