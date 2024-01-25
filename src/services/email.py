import abc
import smtplib


class AbstractEmailSender(abc.ABC):
    def send(self, recipient, message, attachment):
        raise NotImplementedError


class ForecastEmailSender(AbstractEmailSender):
    def __init__(self):
        pass

    def send(self, recipient, message, attachment):
        pass