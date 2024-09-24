import io
import logging
import abc
import smtplib

from pandera.typing import DataFrame

from src.config import settings
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from traceback import format_exception

from src.utils.dataframe_schemas import TimeSeriesSchema, FahrplanmanagementSchema

logger = logging.getLogger(__name__)


class AbstractEmailSender(abc.ABC):
    @abc.abstractmethod
    def send(self, recipient: str, file_name: str, data: DataFrame[TimeSeriesSchema]) -> bool:
        # return True if email was sent successfully, False otherwise
        raise NotImplementedError


class ForecastEmailSender(AbstractEmailSender):
    def __init__(self):
        self.smtp_connection = smtplib.SMTP(settings.smtp_host, settings.smtp_port)
        self.smtp_mail = settings.smtp_email
        self.smtp_pass = settings.smtp_pass

    def send(self, recipient: str, file_name: str, data: DataFrame[FahrplanmanagementSchema]) -> bool:
        msg = MIMEMultipart()
        msg["From"] = self.smtp_mail
        msg["To"] = recipient
        msg["Subject"] = file_name

        buffer = self._to_csv(data)
        attachment = MIMEApplication(buffer.getvalue(), Name=file_name)
        attachment["Content-Disposition"] = f"attachment; filename={file_name}"
        msg.attach(attachment)

        if not settings.send_predictions_enabled:
            return True

        send_errors = None
        try:
            self.smtp_connection.starttls()
            self.smtp_connection.login(self.smtp_mail, self.smtp_pass)
            send_errors = self.smtp_connection.sendmail(self.smtp_mail, recipient, msg.as_string())
        except Exception as exc:
            formatted_exception = "".join(
                format_exception(type(exc), exc, exc.__traceback__)
            )
            logger.error(formatted_exception)
        finally:
            self.smtp_connection.quit()
        return send_errors == {}

    def _to_csv(self, data: DataFrame[TimeSeriesSchema]) -> io.BytesIO:
        file_obj = io.BytesIO()
        data.index = data.index.strftime("%Y-%m-%d %H:%M")
        data.to_csv(file_obj, index=True, sep=";")
        file_obj.seek(0)
        return file_obj
