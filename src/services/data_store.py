import abc
import io
import os

from .email import ForecastEmailSender
from src.domain.model import Prediction


class AbstractDataStore(abc.ABC):
    @abc.abstractmethod
    def save_file(self, prediction: Prediction, *args, **kwargs):
        raise NotImplementedError()


class EmailDataStore(AbstractDataStore):
    def save_file(self, prediction, file_name, buffer, recipient):
        email_service = ForecastEmailSender()
        email_service.send(recipient, file_name, buffer)


class LocalDataStore(AbstractDataStore):
    def save_file(self, prediction: Prediction, *args, **kwargs):
        malo = kwargs.get("malo")
        buffer = io.BytesIO()
        prediction.df.to_csv(
            buffer,
            sep=";",
            index_label="Timestamp (Europe/Berlin)",
            header=[malo],
        )
        buffer.seek(0)

        path = os.path.join(
            "gen_pred_files", f"{malo}_{prediction.updated.strftime('%d%m%Y_%H%M')}.csv"
        )
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, "wb") as f:
            f.write(buffer.getbuffer())
