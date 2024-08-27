from __future__ import annotations

import logging
import datetime
from uuid import UUID

import src.enums
from src.config import settings
from src.domain import commands, events
from src.domain import model
from src.domain.model import MarketLocation
from src.infrastructure import unit_of_work
from src.services import predictor, load_data, data_store
from src.utils.timezone import TIMEZONE_BERLIN

logger = logging.getLogger(__name__)


class InvalidId(Exception):
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

        def get_historic_load_data(malo: MarketLocation):
            result = None
            try:
                df = ldr.get_data(malo.number, malo.measurand)
                result = model.HistoricLoadData(df=df)
            except Exception as exc:
                logger.error("Could not get historic data for market_location %s", malo)
                logger.error(exc)
            return result

        location: model.Location = uow.locations.get(UUID(cmd.location_id))

        if (hld := get_historic_load_data(location.residual_short)) is not None:
            location.residual_short.historic_load_data = hld

        if location.has_production:
            if (hld := get_historic_load_data(location.residual_long)) is not None:
                location.residual_long.historic_load_data = hld

        for producer in location.producers:
            if (hld := get_historic_load_data(producer.market_location)) is not None:
                producer.market_location.historic_load_data = hld

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

        start_date = datetime.date.today() + datetime.timedelta(days=1)
        end_date = start_date + datetime.timedelta(days=7)

        if (
            location.settings.active_until is not None
            and start_date > location.settings.active_until
        ):
            logger.info(
                msg="Won't calculate predictions for location as <active_until> is in the past",
                location=location.alias,
                active_until=location.settings.active_until,
            )
            return
        if end_date < location.settings.active_from:
            logger.info(
                msg="Won't calculate predictions for location as <active_from> is beyond the prediction horizon",
                location=location.alias,
                active_from=location.settings.active_from,
            )
            return
        start_date = max(start_date, location.settings.active_from)
        if location.settings.active_until is not None:
            end_date = min(end_date, location.settings.active_until)

        predictor_setting = predictor.PredictorSettings(
            state=location.state,
            output_period=predictor.Period(
                start=datetime.datetime.combine(start_date, datetime.time.min, tzinfo=TIMEZONE_BERLIN),
                end=datetime.datetime.combine(end_date, datetime.time.max, tzinfo=TIMEZONE_BERLIN)
            ),
        )
        rf_predictor = predictor.RandomForestRegressionPredictor(
            input_df=local_consumption_df, settings=predictor_setting
        )
        try:
            rf_predictor.create_prediction()

            local_consumption_prediction_df = rf_predictor.get_result()

            location.add_prediction(
                model.Prediction(
                    df=local_consumption_prediction_df,
                    type=src.enums.PredictionType.CONSUMPTION,
                )
            )

            # Erzeuerungsprognose Enercast
            if location.has_production:
                enercast_data_retriever = load_data.EnercastSftpDataRetriever()
                for producer in location.producers:
                    location.add_prediction(
                        model.Prediction(
                            df=enercast_data_retriever.get_data(
                                market_location_number=producer.market_location.number
                            ),
                            type=src.enums.PredictionType.PRODUCTION
                        )
                    )

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
                src.enums.PredictionType.RESIDUAL_SHORT
            )
            if short_prediction:
                dst.save_file(short_prediction, malo=location.residual_short.malo)


def add_location(cmd: commands.CreateLocation, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        location = model.Location(
            state=cmd.state,
            alias=cmd.alias,
            residual_short=model.MarketLocation(
                number=cmd.residual_short_malo,
                measurand=src.enums.Measurand.POSITIVE,
            ),
            residual_long=model.MarketLocation(
                number=cmd.residual_long_malo,
                measurand=src.enums.Measurand.NEGATIVE,
            ) if cmd.residual_long_malo else None,
            producers=[model.Producer(
                market_location=model.MarketLocation(
                    number=p["market_location_number"],
                    measurand=src.enums.Measurand.NEGATIVE,
                ),
                prognosis_data_retriever=p["prognosis_data_retriever"]
            ) for p in cmd.producers],
            settings=model.LocationSettings(
                active_from=cmd.settings_active_from,
                active_until=cmd.settings_active_until,
            ),
        )
        uow.locations.add(location)
        uow.commit()
        return location


def update_location_settings(
    cmd: commands.UpdateLocationSettings, uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        location: model.Location = uow.locations.get(UUID(cmd.location_id))

        location.settings = model.LocationSettings(
            active_from=cmd.settings_active_from, active_until=cmd.settings_active_until
        )
        uow.locations.update(location)
        uow.commit()
        return location


EVENT_HANDLERS = {
    events.PredictionsCreated: [send_predictions_evt]
}

COMMAND_HANDLERS = {
    commands.CreateLocation: add_location,
    commands.UpdateLocationSettings: update_location_settings,
    commands.UpdateHistoricData: update_historic_data,
    commands.CalculatePredictions: calculate_predictions,
    commands.SendPredictions: send_predictions,
    commands.UpdatePredictAll: update_and_predict_all,
}
