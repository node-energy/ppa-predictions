from dataclasses import dataclass
from typing import Callable, Type

from src.domain.model import Producer, Location
from src.enums import DataRetriever
from src.services.load_data_exchange.common import AbstractLoadDataRetriever
from src.services.load_data_exchange.enercast import EnercastSftpDataRetriever, EnercastApiDataRetriever
from src.services.load_data_exchange.impuls_energy_trading import IetSftpGenerationDataRetriever


@dataclass
class LocationAndProducer:
    location: Location
    producer: Producer


@dataclass
class DataRetrieverConfig:
    data_retriever: Type[AbstractLoadDataRetriever]
    asset_identifier_func: Callable[[LocationAndProducer], str]


DATA_RETRIEVER_MAP: dict[DataRetriever, DataRetrieverConfig] = {
    DataRetriever.ENERCAST_SFTP: DataRetrieverConfig(
        EnercastSftpDataRetriever,
        lambda location_and_producer: location_and_producer.producer.market_location.number,
    ),
    DataRetriever.ENERCAST_API: DataRetrieverConfig(
        EnercastApiDataRetriever,
        lambda location_and_producer: location_and_producer.producer.market_location.number,
    ),
    DataRetriever.IMPULS_ENERGY_TRADING_SFTP: DataRetrieverConfig(
        IetSftpGenerationDataRetriever,
        lambda location_and_producer: str(location_and_producer.location.residual_long.id),
    )
}


