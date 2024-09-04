from __future__ import annotations

from enum import Enum


class ComponentType(str, Enum):
    PRODUCER = "producer"
    CONSUMER = "consumer"


class Measurand(str, Enum):
    POSITIVE = "positive"   # grid withdrawal / consumption
    NEGATIVE = "negative"   # grid feed-in / generation


class DataRetriever(str, Enum):
    # prognosis data retrievers
    ENERCAST_SFTP = "enercast_sftp"
    ENERCAST_API = "enercast_api"
    IMPULS_ENERGY_TRADING_SFTP = "impuls_energy_trading_sftp"


class PredictionType(str, Enum):
    CONSUMPTION = "consumption"
    PRODUCTION = "production"
    RESIDUAL_SHORT = "short"
    RESIDUAL_LONG = "long"


class State(str, Enum):
    baden_wurttemberg = "BW"
    bayern = "BY"
    berlin = "BE"
    brandenburg = "BB"
    bremen = "HB"
    hamburg = "HH"
    hessen = "HE"
    mecklenburg_vorpommern = "MV"
    niedersachsen = "NI"
    nordrhein_westfalen = "NW"
    rheinland_pfalz = "RP"
    saarland = "SL"
    sachsen = "SN"
    sachsen_anhalt = "ST"
    schleswig_holstein = "SH"
    thuringen = "TH"


class PredictionReceiver(str, Enum):
    INTERNAL_FAHRPLANMANAGEMENT = "internal_fahrplanmanagement"
    IMPULS_ENERGY_TRADING = "impuls_energy_trading"
