import abc
import numpy as np
import pandas
import pandas as pd
from datetime import datetime
from holidays import country_holidays
from typing import Optional
from dataclasses import dataclass
from enum import Enum
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split, GridSearchCV

# from src.domain.value_objects import Period


@dataclass
class Period:
    start: datetime
    end: datetime

    def __post_init__(self):
        self.start = self.start.replace(hour=0, minute=0, second=0, microsecond=0)
        self.end = self.end.replace(hour=0, minute=0, second=0, microsecond=0)


class Entity:
    pass


class PredictionType(Enum):
    CONSUMPTION = 0
    GENERATION = 1


class State(str, Enum):
    BADEN_WURTTEMBERG = "BW"
    BAYERN = "BY"
    BERLIN = "BE"
    BRANDENBURG = "BB"
    BREMEN = "HB"
    HAMBURG = "HH"
    HESSEN = "HE"
    MECKLENBURG_VORPOMMERN = "MV"
    NIEDERSACHSEN = "NI"
    NORDRHEIN_WESTFALEN = "NW"
    RHEINLAND_PFALZ = "RP"
    SAARLAND = "SL"
    SACHSEN = "SN"
    SACHSEN_ANHALT = "ST"
    SCHLESWIG_HOLSTEIN = "SH"
    THURINGEN = "TH"


class Unit(str, Enum):
    kWh = "kWh"


@dataclass
class PredictorSettings(Entity):
    state: State
    output_period: Period
    input_period: Optional[Period] = None
    type: PredictionType = PredictionType.CONSUMPTION
    unit: Unit = Unit.kWh


class AbstractPredictor(abc.ABC):
    def __init__(self, input_df: pd.DataFrame, settings: PredictorSettings):
        self.input_df = input_df
        self.settings = settings
        self.future_df: Optional[pd.DataFrame] = None
        self.rmse: Optional[float] = None

    def _create_future_df(self):
        future_df = pd.DataFrame(
            index=pd.date_range(
                start=self.settings.output_period.start,
                end=self.settings.output_period.end,
                freq=pd.offsets.Minute(15),
                inclusive="left",  # end is exclusive
            )
        )
        return self._add_input_fields(future_df)

    def _add_input_fields(
        self, df: pd.DataFrame
    ):  # assume df consists of datetime index and value
        df["day_of_week"] = pd.to_datetime(df.index).dayofweek
        df["is_weekday"] = np.where(df["day_of_week"] < 5, True, False)
        df["is_friday"] = np.where(df["day_of_week"] == 4, True, False)
        df["is_saturday"] = np.where(df["day_of_week"] == 5, True, False)
        df["is_sunday"] = np.where(df["day_of_week"] == 6, True, False)
        df["month"] = pd.to_datetime(df.index).month
        df["hour"] = df.index.to_series().apply(lambda d: d.hour + d.minute / 60)
        df["nighttime"] = np.cos(2 * np.pi * df["hour"] / 24)
        state_holidays = country_holidays("DE", subdiv=self.settings.state)
        df["is_holiday"] = df.index.to_series().apply(lambda d: d in state_holidays)
        return df

    def _split_data(self):
        x = self.input_df[
            [
                "month",
                "hour",
                "is_weekday",
                "is_friday",
                "is_saturday",
                "is_sunday",
                "nighttime",
                "is_holiday",
            ]
        ]
        y = self.input_df["value"]
        return train_test_split(x, y, test_size=0.2, random_state=42)

    def get_result(self):
        return self.future_df

    def get_rmse(self):
        return

    @abc.abstractmethod
    def create_prediction(self):
        raise NotImplementedError()


class RandomForestRegressionPredictor(AbstractPredictor):
    def create_prediction(self):
        # Slice df to period if input period given TODO
        if self.settings.input_period:
            pass

        self.input_df.dropna(inplace=True)

        # Add X to input_df
        self.input_df = self._add_input_fields(self.input_df)

        # split into train and test
        x_train, x_test, y_train, y_test = self._split_data()

        # create model TODO this can probably be improved
        param_grid = {"n_estimators": [100], "max_depth": [10], "random_state": [56]}
        grid_search = GridSearchCV(RandomForestRegressor(), param_grid, cv=5)
        grid_search.fit(x_train, y_train)
        rfr = RandomForestRegressor(**grid_search.best_params_)
        rfr.fit(x_train, y_train)

        # create future df / predicted df
        # Add X to future df
        future_df = self._create_future_df()

        # use model on future df
        future_df = future_df[
            [
                "month",
                "hour",
                "is_weekday",
                "is_friday",
                "is_saturday",
                "is_sunday",
                "nighttime",
                "is_holiday",
            ]
        ]

        # holidays should use values of sundays
        df: pandas.DataFrame = self.input_df.loc[self.input_df["is_sunday"] == True]
        mean_per_hour = df.groupby(["hour"], as_index=False)["value"].mean()
        sunday_future_df = pd.merge(future_df, mean_per_hour, on="hour", how="left")

        future_prediction_s = pd.Series(rfr.predict(future_df))
        future_df["value"] = np.where(
            future_df["is_holiday"] == False,
            future_prediction_s,
            sunday_future_df["value"],
        )
        self.future_df = future_df["value"].round(3)

        # TODO rmse?
        # rmse_npa = rfr.predict(x_train)
        # rmse_1 = mean_squared_error(y_train, rmse_npa, squared=False)

        # print(f"{rmse_1}")
