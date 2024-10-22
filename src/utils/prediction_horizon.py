import datetime


class PredictionHorizonBase:
    def __init__(self, days_to_future: int):
        self.days_to_future = days_to_future

    @property
    def start_date(self) -> datetime.date:
        return datetime.date.today() + datetime.timedelta(days=1)

    @property
    def end_date(self) -> datetime.date:
        return datetime.date.today() + datetime.timedelta(days=self.days_to_future)

    def dates_in_prediction_horizon(self) -> list[datetime.date]:
        dates = []
        current_date = self.start_date
        while current_date <= self.end_date:
            dates.append(current_date)
            current_date += datetime.timedelta(days=1)
        return dates


class PredictionHorizon(PredictionHorizonBase):
    def __init__(self):
        super().__init__(days_to_future=7)


class PredictionHorizonImpuls(PredictionHorizonBase):
    def __init__(self):
        super().__init__(days_to_future=6)
