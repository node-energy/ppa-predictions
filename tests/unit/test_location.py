import datetime
import datetime as dt
import pandas as pd
from pandas.testing import assert_frame_equal
from src.domain.model import (
    Consumer,
    HistoricLoadData,
    Location,
    Prediction,
    PredictionType,
    Producer,
    State,
)


def create_df_with_constant_values(value=42):
    start = dt.datetime.now().replace(microsecond=0, second=0, minute=0)
    end = start + dt.timedelta(days=30)
    df = pd.DataFrame(
        {"datetime": pd.date_range(start=start, end=end, freq="15min"), "value": value}
    )
    df.set_index("datetime", inplace=True)
    return df


class TestLocation:
    def test_only_one_producer_per_location(self, location):
        producer = Producer(malo="MALO-PRODUCER-01")
        location.add_component(producer)
        producer2 = Producer(malo="MALO_PRODUCER-02")
        location.add_component(producer2)
        assert producer in location.producers
        assert producer2 not in location.producers

    def test_calculate_local_consumption_consumer_only(self, location):
        residual_short_df = create_df_with_constant_values()
        residual_short_load_data = HistoricLoadData(df=residual_short_df)
        location.residual_short.historic_load_data = residual_short_load_data
        result = location.calculate_local_consumption()
        assert_frame_equal(residual_short_df, result)

    def test_calculate_local_consumption_with_producer(self, location, producer):
        input_df = create_df_with_constant_values()
        producer.historic_load_data = HistoricLoadData(df=input_df)
        location.producers.append(producer)
        location.residual_short.historic_load_data = HistoricLoadData(df=input_df)
        result = location.calculate_local_consumption()
        assert (result["value"] == 84).all() == True

    def test_calculate_local_consumption_with_producer_and_residual_long(
        self, location, producer
    ):
        input_df = create_df_with_constant_values()
        producer.historic_load_data = HistoricLoadData(df=input_df)
        location.producers.append(producer)
        location.residual_short.historic_load_data = HistoricLoadData(
            df=create_df_with_constant_values(0)
        )
        location.residual_long = Producer(
            malo="MALO-PRODUCER-1", historic_load_data=HistoricLoadData(df=input_df)
        )
        result = location.calculate_local_consumption()
        assert (result["value"] == 0).all() == True

    def test_calculate_location_residual_loads_consumer_only(self, location):
        input_df = create_df_with_constant_values()
        location.predictions.append(
            Prediction(df=input_df, type=PredictionType.CONSUMPTION)
        )
        location.calculate_location_residual_loads()
        prediction_residual_short = next(
            p for p in location.predictions if p.type == PredictionType.RESIDUAL_SHORT
        )
        assert_frame_equal(prediction_residual_short.df, input_df)

    def test_delete_oldest_predictions(self, location: Location):
        oldest = Prediction(
            df=create_df_with_constant_values(),
            type=PredictionType.CONSUMPTION,
            updated=datetime.datetime(2024, 1, 1),
        )
        location.predictions.append(oldest)
        newest = Prediction(
            df=create_df_with_constant_values(),
            type=PredictionType.CONSUMPTION,
            updated=datetime.datetime(2024, 1, 2),
        )
        location.predictions.append(newest)

        location.delete_oldest_predictions(keep=1, type=PredictionType.CONSUMPTION)

        assert location.predictions == [newest]

    def test_delete_only_specific_prediction_types(self, location: Location):
        consumption_prediction = Prediction(
            df=create_df_with_constant_values(),
            type=PredictionType.CONSUMPTION,
            updated=datetime.datetime(2024, 1, 1),
        )
        consumption_prediction_2 = Prediction(
            df=create_df_with_constant_values(),
            type=PredictionType.CONSUMPTION,
            updated=datetime.datetime(2024, 1, 2),
        )
        production_prediction = Prediction(
            df=create_df_with_constant_values(),
            type=PredictionType.PRODUCTION,
            updated=datetime.datetime(2024, 1, 2),
        )
        location.predictions.append(consumption_prediction)
        location.predictions.append(consumption_prediction_2)
        location.predictions.append(production_prediction)

        location.delete_oldest_predictions(type=PredictionType.CONSUMPTION, keep=1)

        assert set(location.predictions) == {production_prediction, consumption_prediction_2}

    def test_delete_and_keep_none(self, location: Location):
        consumption_prediction = Prediction(
            df=create_df_with_constant_values(),
            type=PredictionType.CONSUMPTION,
            updated=datetime.datetime(2024, 1, 1),
        )
        production_prediction = Prediction(
            df=create_df_with_constant_values(),
            type=PredictionType.PRODUCTION,
            updated=datetime.datetime(2024, 1, 2),
        )
        location.predictions.append(consumption_prediction)
        location.predictions.append(production_prediction)

        location.delete_oldest_predictions(type=PredictionType.CONSUMPTION, keep=0)

        assert location.predictions == [production_prediction]
