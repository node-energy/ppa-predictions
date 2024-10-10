import datetime as dt

from src.utils.external_schedules import GATE_CLOSURE_INTERNAL_FAHRPLANMANAGEMENT

ONE_HOUR_BEFORE_GATE_CLOSURE = dt.datetime.combine(
    dt.date.today(),
    GATE_CLOSURE_INTERNAL_FAHRPLANMANAGEMENT
) - dt.timedelta(hours=1)
