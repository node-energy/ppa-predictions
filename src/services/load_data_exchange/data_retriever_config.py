from dataclasses import dataclass
from typing import Callable, Type

from src.domain.model import Producer
from src.enums import DataRetriever
from src.services.load_data_exchange.common import AbstractLoadDataRetriever
from src.services.load_data_exchange.enercast import EnercastSftpDataRetriever, EnercastApiDataRetriever
from src.services.load_data_exchange.impuls_energy_trading import IetSftpGenerationDataRetriever


@dataclass
class DataRetrieverConfig:
    data_retriever: Type[AbstractLoadDataRetriever]
    asset_identifier_func: Callable[[Producer], str]


DATA_RETRIEVER_MAP: dict[DataRetriever, DataRetrieverConfig] = {
    DataRetriever.ENERCAST_SFTP: DataRetrieverConfig(
        EnercastSftpDataRetriever,
        lambda producer: producer.market_location.number,
    ),
    DataRetriever.ENERCAST_API: DataRetrieverConfig(
        EnercastApiDataRetriever,
        lambda producer: producer.market_location.number,
    ),
    DataRetriever.IMPULS_ENERGY_TRADING_SFTP: DataRetrieverConfig(
        IetSftpGenerationDataRetriever,
        lambda producer: str(producer.id),
    )
}


