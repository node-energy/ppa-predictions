import abc
import datetime
from datetime import date
import holidays
import pandas as pd
import numpy as np
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import train_test_split, GridSearchCV
from src.domain.model import HistoricLoadProfile


class AbstractPredictor(abc.ABC):
    @abc.abstractmethod
    def create_prediction(self):
        raise NotImplementedError


DEFAULT_NUM_ESTIMATORS = 100
DEFAULT_MAX_DEPTH = 10
DEFAULT_RANDOM_STATE = 56


class SimplePredictor(AbstractPredictor):
    def configure(
        self, historic_load_profile_slice: HistoricLoadProfile, state, predict_days=7
    ):
        self._historic_load_prodile_slice = historic_load_profile_slice
        self._state = state
        self._predict_days = predict_days

    def create_prediction(self):
        historic_df = self._get_df()
        start_date = datetime.datetime.now()
        end_date = start_date + datetime.timedelta(days=self._predict_days)
        future_datetimes = pd.date_range(start_date, end_date, freq="15min").tolist()
        future_df = pd.DataFrame({"datetime": future_datetimes})
        future_df = pd.concat([future_df, historic_df], axis="columns")
        return future_df
        # get time series interval
        # for each day get weekday
        # for each day check if holiday
        # for each day get hour consumption
        #  move data to fit year (weekdays, fridays, weekends, time, holidays)
        #  fill missing data

    def _get_df(self):
        historic_load_profile_slice = self._historic_load_prodile_slice
        data = list(
            map(lambda t: [t.datetime, t.value], historic_load_profile_slice.timestamps)
        )
        historic_df = pd.DataFrame(data, columns=["datetime", "value"])
        historic_df = historic_df.set_index("datetime")
        historic_df["day_of_week"] = pd.to_datetime(historic_df.index).dayofweek
        state_holidays = holidays.country_holidays("DE", subdiv=self._state)
        historic_df["is_holiday"] = historic_df.index.to_series().apply(
            lambda d: d in state_holidays
        )
        historic_df["is_weekday"] = np.where(
            historic_df["day_of_week"] < 5, True, False
        )
        historic_df["is_friday"] = np.where(
            historic_df["day_of_week"] == 4, True, False
        )
        historic_df["is_saturday"] = np.where(
            historic_df["day_of_week"] == 5, True, False
        )
        historic_df["is_sunday"] = np.where(
            historic_df["day_of_week"] == 6, True, False
        )
        historic_df["month"] = pd.to_datetime(historic_df.index).month
        historic_df["hour"] = historic_df.index.to_series().apply(
            lambda d: d.hour + d.minute / 60
        )
        historic_df["nighttime"] = np.cos(2 * np.pi * historic_df["hour"] / 24)
        return historic_df


class RandomForestPredictor(SimplePredictor):
    def __init__(
        self,
        num_estimators=DEFAULT_NUM_ESTIMATORS,
        max_depth=DEFAULT_MAX_DEPTH,
        random_state=DEFAULT_RANDOM_STATE,
    ):
        self.x = [
                "month",
                "hour",
                "is_weekday",
                "is_friday",
                "is_saturday",
                "is_sunday",
                "nighttime",
                "is_holiday",
            ]
        self.random_forest_regressor = RandomForestRegressor(
            n_estimators=num_estimators, max_depth=max_depth, random_state=random_state
        )

    def _define_x_for_data(self, df):
        df["day_of_week"] = pd.to_datetime(df.index).dayofweek
        state_holidays = holidays.country_holidays("DE", subdiv=self._state)
        df["is_holiday"] = df.index.to_series().apply(lambda d: d in state_holidays)
        df["is_weekday"] = np.where(df["day_of_week"] < 5, True, False)
        df["is_friday"] = np.where(df["day_of_week"] == 4, True, False)
        df["is_saturday"] = np.where(df["day_of_week"] == 5, True, False)
        df["is_sunday"] = np.where(df["day_of_week"] == 6, True, False)
        df["month"] = pd.to_datetime(df.index).month
        df["hour"] = df.index.to_series().apply(lambda d: d.hour + d.minute / 60)
        df["nighttime"] = np.cos(2 * np.pi * df["hour"] / 24)
        return df

    def _get_trained_rfr(self, df):
        x = df[self.x]
        y = df["value"]
        x_train, x_test, y_train, y_test = train_test_split(
            x, y, test_size=0.2, random_state=0
        )
        rfr = RandomForestRegressor()
        param_grid = {"n_estimators": [100], "max_depth": [10], "random_state": [56]}
        grid_search = GridSearchCV(rfr, param_grid, cv=5)
        grid_search.fit(x_train, y_train)
        params = grid_search.best_params_
        rfr = RandomForestRegressor(**params)
        rfr.fit(x_train, y_train)
        return rfr

    def create_prediction(self):
        historic_load_profile_slice = self._historic_load_prodile_slice
        data = list(
            map(lambda t: [t.datetime, t.value], historic_load_profile_slice.timestamps)
        )
        historic_df = pd.DataFrame(data, columns=["datetime", "value"])
        historic_df = historic_df.set_index("datetime")
        historic_df_with_x = self._define_x_for_data(historic_df)
        rfr = self._get_trained_rfr(historic_df_with_x)

        # create future df
        got_values_until = historic_df.index.max()
        end_date = got_values_until + datetime.timedelta(days=self._predict_days)
        future_datetimes = pd.date_range(
            got_values_until, end_date, freq="15min"
        ).tolist()

        future_df = pd.DataFrame({"datetime": future_datetimes})
        future_df = future_df.set_index("datetime")
        future_df = self._define_x_for_data(future_df)
        x_future = future_df[self.x]

        y_future = rfr.predict(x_future)
        future_df["RFR"] = y_future
        print(future_df.tail(200))
        return future_df


class PredictPlusPredictor(AbstractPredictor):
    def create_prediction(self):
        pass


#  TODO Use multiple predictors and weight them
#  TODO Evaluate by using mean absolute percentage error
#  TODO ML Models: XGBoost, (Non-)linear Regressors, SVMs, Clustering
