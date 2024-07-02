from __future__ import annotations

import logging
import datetime
from traceback import format_exception
from uuid import uuid4, UUID
from src.config import settings
from src.domain import commands, events
from src.domain import model
from src.infrastructure import unit_of_work
from src.services import predictor, load_data, data_store


logger = logging.getLogger(__name__)


class InvalidRef(Exception):
    pass


def update_and_predict_all(
    _: commands.UpdatePredictAll,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: load_data.AbstractLoadDataRetriever,
    dst: data_store.AbstractDataStore,
):
    with uow:
        for location in uow.locations.get_all():
            update_historic_data(
                commands.UpdateHistoricData(location_id=str(location.id)), uow, ldr
            )
            calculate_predictions(
                commands.CalculatePredictions(location_id=str(location.id)), uow
            )
            send_predictions(
                commands.SendPredictions(location_id=str(location.id)), uow, dst
            )


def update_historic_data(
    cmd: commands.UpdateHistoricData,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: load_data.AbstractLoadDataRetriever,
):
    with uow:

        def get_historic_load_data(malo: str):
            result = None
            try:
                df = ldr.get_data(malo)
                result = model.HistoricLoadData(df=df)
            except:
                logger.error("Could not get historic data for malo %s", malo)
            return result

        location: model.Location = uow.locations.get(UUID(cmd.location_id))

        # historic_load_data_residual_short_df = ldr.get_data(
        #     location.residual_short.malo
        # )

        if (hld := get_historic_load_data(location.residual_short.malo)) is not None:
            location.residual_short.historic_load_data = hld

        # location.residual_short.historic_load_data = get_historic_load_data(
        #     location.residual_short.malo
        # )
        #
        # location.residual_short.historic_load_data = model.HistoricLoadData(
        #     df=historic_load_data_residual_short_df
        # )

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
        uow.commit()


def calculate_predictions(
    cmd: commands.CalculatePredictions,
    uow: unit_of_work.AbstractUnitOfWork,
):
    with uow:
        location: model.Location = uow.locations.get(UUID(cmd.location_id))

        # Verbrauchsprognose
        local_consumption_df = location.calculate_local_consumption()
        if local_consumption_df is None:
            return

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

        location.add_prediction(
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
        uow.commit()


def send_predictions_evt(
    evt: events.PredictionsCreated,
    uow: unit_of_work,
    dst: data_store.AbstractDataStore,
):
    send_predictions(commands.SendPredictions(location_id=evt.location_id), uow, dst)


def send_predictions(
    cmd: commands.SendPredictions,
    uow: unit_of_work.AbstractUnitOfWork,
    dst: data_store.AbstractDataStore,
):
    if settings.send_predictions_enabled:
        with uow:
            location: model.Location = uow.locations.get(UUID(cmd.location_id))
            short_prediction = location.get_most_recent_prediction(
                model.PredictionType.RESIDUAL_SHORT
            )
            if short_prediction:
                dst.save_file(short_prediction, malo=location.residual_short.malo)


def add_location(cmd: commands.CreateLocation, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        location = model.Location(
            state=cmd.state, residual_short=model.Consumer(malo=cmd.residual_short_malo)
        )
        uow.locations.add(location)
        uow.commit()
        return location


EVENT_HANDLERS = {
    # events.CustomerCreated: [test_handler],
    # events.HistoricLoadProfileReceived: [create_prediction],
    # events.Predict: [start_prediction],
    # events.DataUpdateRequired: [update_data],
    # events.DataUpdated: [create_prediction],
    # events.PredictionRequired [create_prediction],
    # events.PredictionCreated: [send_prediction],
    events.PredictionsCreated: [send_predictions_evt]
}

COMMAND_HANDLERS = {
    commands.CreateLocation: add_location,
    commands.UpdateHistoricData: update_historic_data,
    commands.CalculatePredictions: calculate_predictions,
    commands.SendPredictions: send_predictions,
    commands.UpdatePredictAll: update_and_predict_all,
}
