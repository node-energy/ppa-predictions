import datetime

from src.utils.timezone import TIMEZONE_BERLIN


GATE_CLOSURE_INTERNAL_FAHRPLANMANAGEMENT = datetime.time(11, 45, tzinfo=TIMEZONE_BERLIN)    # todo verify this!!
GATE_CLOSURE_IMPULS_ENERGY_TRADING = datetime.time(13, 0, tzinfo=TIMEZONE_BERLIN)