import datetime

import pandas as pd

from src.services.load_data_exchange.email import AbstractEmailSender
from src.services.load_data_exchange.common import AbstractLoadDataSender


class FakeEmailSender(AbstractEmailSender):
    def __init__(self):
        self.data = []

    def send(self, recipient: str, file_name: str, buffer):
        self.data.append(buffer.getvalue())


class FakeIetDataSender(AbstractLoadDataSender):
    def __init__(self):
        self.data = {}

    def send_data(self, data: pd.DataFrame, prediction_date: datetime.date):
        self.data[prediction_date] = data
