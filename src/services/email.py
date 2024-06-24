import logging
import abc
import smtplib
from src.config import settings
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication
from traceback import format_exception


logger = logging.getLogger(__name__)


class AbstractEmailSender(abc.ABC):
    @abc.abstractmethod
    def send(self, recipient: str, file_name: str, buffer):
        raise NotImplementedError


class ForecastEmailSender(AbstractEmailSender):
    def __init__(self):
        self.smtp_connection = smtplib.SMTP(settings.smtp_host, settings.smtp_port)
        self.smtp_mail = settings.smtp_email
        self.smtp_pass = settings.smtp_pass

    def send(self, recipient: str, file_name: str, buffer):
        msg = MIMEMultipart()
        msg["From"] = self.smtp_mail
        msg["To"] = recipient
        msg["Subject"] = file_name

        attachment = MIMEApplication(buffer.getvalue(), Name=file_name)
        attachment["Content-Disposition"] = f"attachment; filename={file_name}"
        msg.attach(attachment)

        try:
            self.smtp_connection.starttls()
            self.smtp_connection.login(self.smtp_mail, self.smtp_pass)
            self.smtp_connection.sendmail(self.smtp_mail, recipient, msg.as_string())
        except Exception as exc:
            formatted_exception = "".join(
                format_exception(type(exc), exc, exc.__traceback__)
            )
            logger.error(formatted_exception)
        finally:
            self.smtp_connection.quit()
