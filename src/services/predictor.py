import abc
import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.model_selection import GridSearchCV


class AbstractPredictor(abc.ABC):
    @abc.abstractmethod
    def create_prediction(self):
        raise NotImplementedError


DEFAULT_NUM_ESTIMATORS = 100
DEFAULT_MAX_DEPTH = 10
DEFAULT_RANDOM_STATE = 56


class SimplePredictor(AbstractPredictor):
    def create_prediction(self):
        pass
        #  move data to fit year (weekends)
        #  fill missing data


class RandomForestPredictor(AbstractPredictor):
    def __init__(
        self,
        num_estimators=DEFAULT_NUM_ESTIMATORS,
        max_depth=DEFAULT_MAX_DEPTH,
        random_state=DEFAULT_RANDOM_STATE
    ):
        self.random_forest_regressor = RandomForestRegressor(
            n_estimators=num_estimators,
            max_depth=max_depth,
            random_state=random_state
        )

        #grid_search_cv = GridSearchCV()

    def create_prediction(self):
        pass