import abc
from .email import ForecastEmailSender


class AbstractDataStore(abc.ABC):
    @abc.abstractmethod
    def save_file(self, *args, **kwargs):
        raise NotImplementedError()


class EmailDataStore(AbstractDataStore):
    def save_file(self, file_name, buffer, recipient):
        email_service = ForecastEmailSender()
        email_service.send(recipient, file_name, buffer)


class LocalDataStore(AbstractDataStore):
    def save_file(self, file_name, buffer):
        with open(f"{file_name}.csv", "wb") as f:
            f.write(buffer)
