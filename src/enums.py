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
    BADEN_WURTTEMBERG = "BW"
    BAYERN = "BY"
    BERLIN = "BE"
    BRANDENBURG = "BB"
    BREMEN = "HB"
    HAMBURG = "HH"
    HESSEN = "HE"
    MECKLENBURG_VORPOMMERN = "MV"
    NIEDERSACHSEN = "NI"
    NORDRHEIN_WESTFALEN = "NW"
    RHEINLAND_PFALZ = "RP"
    SAARLAND = "SL"
    SACHSEN = "SN"
    SACHSEN_ANHALT = "ST"
    SCHLESWIG_HOLSTEIN = "SH"
    THURINGEN = "TH"


class PredictionReceiver(str, Enum):
    INTERNAL_FAHRPLANMANAGEMENT = "internal_fahrplanmanagement"
    IMPULS_ENERGY_TRADING = "impuls_energy_trading"


class TransmissionSystemOperator(str, Enum):
    HERTZ = "50hzt"
    AMPRION = "amprion"
    TENNET = "tennet"
    TRANSNET = "transnet"


