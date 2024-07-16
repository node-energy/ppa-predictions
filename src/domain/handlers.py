from __future__ import annotations

import asyncio
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


def update_and_predict_all_impr(
    _: commands.UpdatePredictAll,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: load_data.AbstractLoadDataRetriever,
    dst: data_store.AbstractDataStore,
):
    logger.warning("START")
    with uow:
        locs = uow.locations.get_all()

        for location in locs:
            update_historic_data(
                commands.UpdateHistoricData(location_id=str(location.id)), uow, ldr
            )

        asyncio.run(test1(locs, uow, ldr, dst))
    logger.warning("FINISHED")


async def test1(locations: list[model.Location], uow, ldr, dst):
    async def update_historic_data_async(cmd, uow, ldr):
        update_historic_data(cmd, uow, ldr)

    async def calculate_predictions_async(cmd, uow):
        calculate_predictions(cmd, uow)

    async def send_predictions_async(cmd, uow, dst):
        send_predictions(cmd, uow, dst)

    tasks = set()

    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = []
        for location in locations:
            futures.append(executor.submit(
                calculate_predictions,
                cmd=commands.CalculatePredictions(location_id=str(location)),
                uow=uow
            ))
            for future in concurrent.futures.as_completed(futures):
                print("completed")

    async with asyncio.TaskGroup() as group:
        for location in locations:
            task = group.create_task(
                send_predictions_async(
                    commands.SendPredictions(location_id=str(location.id)), uow, dst
                )
            )
            tasks.add(task)
            task.add_done_callback(tasks.discard)


def update_and_predict_all(
    _: commands.UpdatePredictAll,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: load_data.AbstractLoadDataRetriever,
    dst: data_store.AbstractDataStore,
):
    logger.warning("Start updating data and creating predictions for all locations")
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
    logger.warning("Finished updating data and creating predictions for all locations")


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
            except Exception as exc:
                logger.error("Could not get historic data for malo %s", malo)
                logger.error(exc)
            return result

        location: model.Location = uow.locations.get(UUID(cmd.location_id))

        if (hld := get_historic_load_data(location.residual_short.malo)) is not None:
            location.residual_short.historic_load_data = hld

        if location.has_production:
            if (hld := get_historic_load_data(location.residual_long.malo)) is not None:
                location.residual_long.historic_load_data = hld

        for producer in location.producers:
            if (hld := get_historic_load_data(producer.malo)) is not None:
                producer.historic_load_data = hld

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
        try:
            rf_predictor.create_prediction()

            local_consumption_prediction_df = rf_predictor.get_result()

            #location.delete_oldest_predictions(keep=1)
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
        except Exception as exc:
            logger.error(f"Could not create prediction for location {location.alias}")
            logger.error(exc)

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
            state=cmd.state,
            alias=cmd.alias,
            residual_short=model.Consumer(malo=cmd.residual_short_malo),
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
