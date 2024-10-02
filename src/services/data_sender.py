import abc
import datetime

from pandera.typing import DataFrame

from src.services.load_data_exchange.email import ForecastEmailSender, AbstractEmailSender
from src.services.load_data_exchange.common import AbstractLoadDataSender
from src.services.load_data_exchange.impuls_energy_trading import IetSftpEigenverbrauchDataSender, \
    IetSftpResidualLongDataSender
from src.utils.dataframe_schemas import TimeSeriesSchema


class AbstractDataSender(abc.ABC):
    @abc.abstractmethod
    def send_to_internal_fahrplanmanagement(self, data: DataFrame[TimeSeriesSchema], *args, **kwargs) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def send_eigenverbrauch_to_impuls_energy_trading(self, data, **kwargs) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def send_residual_long_to_impuls_energy_trading(self, data, **kwargs) -> bool:
        raise NotImplementedError()


class DataSender(AbstractDataSender):
    def __init__(
        self,
        fahrplanmanagement_sender: AbstractEmailSender = ForecastEmailSender(),
        impuls_energy_trading_eigenverbrauch_sender: AbstractLoadDataSender = IetSftpEigenverbrauchDataSender(),
        impuls_energy_trading_residual_long_sender: AbstractLoadDataSender = IetSftpResidualLongDataSender()
    ):
        self.fahrplanmanagement_sender = fahrplanmanagement_sender
        self.impuls_energy_trading_eigenverbrauch_sender = impuls_energy_trading_eigenverbrauch_sender
        self.impuls_energy_trading_residual_long_sender = impuls_energy_trading_residual_long_sender

    def send_to_internal_fahrplanmanagement(self, data: DataFrame[TimeSeriesSchema], file_name: str, recipient: str) -> bool:
        successful = self.fahrplanmanagement_sender.send(recipient, file_name, data)
        return successful

    def send_eigenverbrauch_to_impuls_energy_trading(self, data, prediction_date: datetime.date) -> bool:
        successful = self.impuls_energy_trading_eigenverbrauch_sender.send_data(data, prediction_date)
        return successful

    def send_residual_long_to_impuls_energy_trading(self, data, prediction_date: datetime.date) -> bool:
        successful = self.impuls_energy_trading_residual_long_sender.send_data(data, prediction_date)
        return successful
