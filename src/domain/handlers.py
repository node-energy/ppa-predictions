from __future__ import annotations

import io
import logging
import datetime
from uuid import uuid4, UUID
from src.config import settings
from src.domain import commands, events
from src.domain import model
from src.infrastructure import unit_of_work
from src.services import predictor, load_data, data_store


logger = logging.getLogger(__name__)


class InvalidRef(Exception):
    pass


def test_handler(event: events.CustomerCreated):
    print("TEST")


def update_historic_data(
    cmd: commands.UpdateHistoricData,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: load_data.AbstractLoadDataRetriever,
):
    with uow:
        location: model.Location = uow.locations.get(UUID(cmd.location_id))

        historic_load_data_residual_short_df = ldr.get_data(
            location.residual_short.malo
        )
        location.residual_short.historic_load_data = model.HistoricLoadData(
            df=historic_load_data_residual_short_df
        )

        if location.has_production:
            historic_load_data_residual_long_df = ldr.get_data(
                location.residual_long.malo
            )
            location.residual_long.historic_load_data = model.HistoricLoadData(
                df=historic_load_data_residual_long_df
            )

        for producer in location.producers:
            historic_load_data_producer_df = ldr.get_data(producer.malo)
            producer.historic_load_data = model.HistoricLoadData(
                df=historic_load_data_producer_df
            )

        uow.locations.update(location)
        uow.commit()  #  TODO save to DB


def calculate_predictions(
    cmd: commands.CalculatePredictions,
    uow: unit_of_work.AbstractUnitOfWork,
):
    with uow:
        location: model.Location = uow.locations.get(UUID(cmd.location_id))

        # Verbrauchsprognose
        local_consumption_df = location.calculate_local_consumption()

        start_date = datetime.datetime.combine(
            datetime.date.today() + datetime.timedelta(days=1),
            datetime.datetime.min.time(),
        )
        end_date = start_date + datetime.timedelta(days=7)
        predictor_setting = predictor.PredictorSettings(
            state=location.state,
            output_period=predictor.Period(start=start_date, end=end_date),
        )
        rf_predictor = predictor.RandomForestRegressionPredictor(
            input_df=local_consumption_df, settings=predictor_setting
        )
        rf_predictor.create_prediction()
        local_consumption_prediction_df = rf_predictor.get_result()

        location.predictions.append(
            model.Prediction(
                df=local_consumption_prediction_df,
                type=model.PredictionType.CONSUMPTION,
            )
        )

        # Erzeuerungsprognose Enercast
        if location.has_production:
            pass

        # Überschuss / Bezug
        location.calculate_location_residual_loads()
        uow.locations.update(location)
        uow.commit()  #  TODO save to DB


def send_predictions(
    cmd: commands.SendPredictions,
    uow: unit_of_work.AbstractUnitOfWork,
    dst: data_store.AbstractDataStore,
):
    if settings.send_predictions_enabled:
        with uow:
            location: model.Location = uow.locations.get(UUID(cmd.location_id))
            for prediction in location.predictions:
                if prediction.type == model.PredictionType.RESIDUAL_SHORT:  # TODO part of sender
                    buffer = io.BytesIO()
                    prediction.df.to_csv(
                        buffer,
                        sep=";",
                        index_label="Timestamp (Europe/Berlin)",
                        header=[location.residual_short.malo]
                    )
                    buffer.seek(0)
                    dst.save_file(f"{location.residual_short.malo}_short", buffer)


def add_location(cmd: commands.CreateLocation, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        location = model.Location(
            state=cmd.state, residual_short=model.Consumer(malo=cmd.residual_short_malo)
        )
        uow.locations.add(location)
        uow.commit()
        return location


EVENT_HANDLERS = {
    events.CustomerCreated: [test_handler],
    #events.HistoricLoadProfileReceived: [create_prediction],
}

COMMAND_HANDLERS = {
    commands.CreateLocation: add_location,
    # new
    commands.UpdateHistoricData: update_historic_data,
    commands.CalculatePredictions: calculate_predictions,
    commands.SendPredictions: send_predictions,
}
