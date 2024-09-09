import abc

from src.services.load_data_exchange.email import ForecastEmailSender, AbstractEmailSender
from src.domain.model import Prediction
from src.services.load_data_exchange.common import AbstractLoadDataSender
from src.services.load_data_exchange.impuls_energy_trading import IetSftpEigenverbrauchDataSender, \
    IetSftpResidualLongDataSender


class AbstractDataSender(abc.ABC):
    @abc.abstractmethod
    def send_to_internal_fahrplanmanagement(self, prediction: Prediction, *args, **kwargs) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def send_eigenverbrauch_to_impuls_energy_trading(self, data) -> bool:
        raise NotImplementedError()

    @abc.abstractmethod
    def send_residual_long_to_impuls_energy_trading(self, data) -> bool:
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

    def send_to_internal_fahrplanmanagement(self, prediction: Prediction, file_name: str, recipient: str, buffer) -> bool:
        successful = self.fahrplanmanagement_sender.send(recipient, file_name, buffer)
        return successful

    def send_eigenverbrauch_to_impuls_energy_trading(self, data) -> bool:
        successful = self.impuls_energy_trading_eigenverbrauch_sender.send_data(data)
        return successful

    def send_residual_long_to_impuls_energy_trading(self, data) -> bool:
        successful = self.impuls_energy_trading_residual_long_sender.send_data(data)
        return successful
