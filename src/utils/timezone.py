from __future__ import annotations

import zoneinfo
import datetime

TIMEZONE_BERLIN = zoneinfo.ZoneInfo("Europe/Berlin")
TIMEZONE_UTC = zoneinfo.ZoneInfo("UTC")


def utc_now():
    return datetime.datetime.now(tz=TIMEZONE_UTC)
