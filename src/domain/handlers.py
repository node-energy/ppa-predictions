from __future__ import annotations

import logging
import datetime
import uuid
from collections import OrderedDict
from typing import Optional
from uuid import UUID

import pandas as pd
from pandera.typing import DataFrame

import src.enums
import src.services.load_data_exchange.common
from src.config import settings
from src.domain import commands, events
from src.domain import model
from src.domain.model import MarketLocation, PredictionShipment
from src.infrastructure import unit_of_work
from src.services import predictor, data_sender
from src.services.load_data_exchange.data_retriever_config import DATA_RETRIEVER_MAP, LocationAndProducer
from src.services.load_data_exchange.impuls_energy_trading import TIMEZONE_FILENAMES
from src.utils.dataframe_schemas import IetLoadDataSchema, TimeSeriesSchema, FahrplanmanagementSchema
from src.utils.external_schedules import GATE_CLOSURE_INTERNAL_FAHRPLANMANAGEMENT
from src.utils.split_df_by_day import split_df_by_day
from src.utils.timezone import TIMEZONE_BERLIN, TIMEZONE_UTC
from src.enums import Measurand, DataRetriever, PredictionType
from src import enums

logger = logging.getLogger(__name__)


class InvalidId(Exception):
    pass


def update_and_predict_all(
    _: commands.UpdatePredictAll,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: src.services.load_data_exchange.common.AbstractLoadDataRetriever,
    dts: data_sender.AbstractDataSender,
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
                commands.SendPredictions(location_id=str(location.id)), uow, dts
            )


def update_historic_data(
    cmd: commands.UpdateHistoricData,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: src.services.load_data_exchange.common.AbstractLoadDataRetriever,
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
            input_period=predictor.Period(
                start=datetime.datetime.combine(
                    start_date - datetime.timedelta(days=location.settings.historic_days_for_consumption_prediction), datetime.time.min, tzinfo=TIMEZONE_BERLIN
                ),
                end=datetime.datetime.combine(
                    start_date - datetime.timedelta(days=1), datetime.time.max, tzinfo=TIMEZONE_BERLIN
                )
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
                    df=DataFrame[TimeSeriesSchema](local_consumption_prediction_df),
                    type=src.enums.PredictionType.CONSUMPTION,
                )
            )

            # Erzeugungsprognose
            if location.has_production:
                for producer in location.producers:
                    data_retriever_config = DATA_RETRIEVER_MAP[producer.prognosis_data_retriever]
                    data_retriever = data_retriever_config.data_retriever()
                    asset_identifier = data_retriever_config.asset_identifier_func(
                        LocationAndProducer(location, producer)
                    )
                    location.add_prediction(
                        model.Prediction(
                            df=DataFrame[TimeSeriesSchema](data_retriever.get_data(
                                asset_identifier=asset_identifier,
                                measurand=Measurand.NEGATIVE,
                                start=datetime.datetime.combine(
                                    start_date, datetime.time.min, tzinfo=TIMEZONE_BERLIN
                                ),
                            )),
                            type=src.enums.PredictionType.PRODUCTION,
                            component=producer,
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
    dts: data_sender.AbstractDataSender,
):
    send_predictions(commands.SendPredictions(location_id=evt.location_id), uow, dts)


def send_predictions(
    cmd: commands.SendPredictions,
    uow: unit_of_work.AbstractUnitOfWork,
    dts: data_sender.AbstractDataSender,
):
    # this only sends data to internal fahrplanmanagement, because impuls requires one single file for all locations
    # so in case one location was updated, sending jobs for impuls must be triggered additionally
    today = datetime.date.today().strftime("%Y-%m-%d")
    with uow:
        location: model.Location = uow.locations.get(UUID(cmd.location_id))
        short_prediction_sent = False
        if location.settings.send_consumption_predictions_to_fahrplanmanagement:
            short_prediction = location.get_most_recent_prediction(src.enums.PredictionType.RESIDUAL_SHORT)
            short_prediction_df = FahrplanmanagementSchema.from_time_series_schema(short_prediction.df, location.residual_short.number)
            if short_prediction:
                short_prediction_sent = dts.send_to_internal_fahrplanmanagement(
                    data=short_prediction_df,
                    file_name=f"{location.residual_short.number}_{location.alias if location.alias else ''}_residual_short_{today}.csv",
                    recipient=settings.mail_recipient_cons
                )
                if short_prediction_sent:
                    short_prediction.shipments.append(
                        model.PredictionShipment(receiver=enums.PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT)
                    )

        if location.has_production:
            long_prediction = location.get_most_recent_prediction(src.enums.PredictionType.RESIDUAL_LONG)
            long_prediction_df = FahrplanmanagementSchema.from_time_series_schema(long_prediction.df, location.residual_long.number)
            if long_prediction:
                long_prediction_sent = dts.send_to_internal_fahrplanmanagement(
                    data=long_prediction_df,
                    file_name=f"{location.residual_long.number}_{location.alias if location.alias else ''}_residual_long_{today}.csv",
                    recipient=settings.mail_recipient_prod
                )
                if long_prediction_sent:
                    long_prediction.shipments.append(
                        model.PredictionShipment(receiver=enums.PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT)
                    )

        # if residuals where successfully sent, also mark the consumption and production predictions as sent because
        # they are the base for computing the residuals.
        # This is necessary, because for sending own consumption data to impuls we need to compute own consumption
        # based on consumption and production predictions that where used to compute the residuals that where sent to
        # internal fahrplanmanagement.
        # This is not the perfect model, maybe it would be better to store input predictions on residuals.

        consumption_prediction = location.get_most_recent_prediction(src.enums.PredictionType.CONSUMPTION)
        production_prediction = location.get_most_recent_prediction(src.enums.PredictionType.PRODUCTION)
        if not location.has_production:
            if short_prediction_sent:
                consumption_prediction.shipments.append(
                    model.PredictionShipment(receiver=enums.PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT)
                )
        else:
            if short_prediction_sent or long_prediction_sent:
                # both residual_short and residual_long use consumption and production predictions as input
                consumption_prediction.shipments.append(
                    model.PredictionShipment(receiver=enums.PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT)
                )
                production_prediction.shipments.append(
                    model.PredictionShipment(receiver=enums.PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT)
                )
        uow.locations.update(location)
        uow.commit()


def send_eigenverbrauchs_predictions_to_impuls_energy_trading(
    cmd: commands.SendAllEigenverbrauchsPredictionsToImpuls,
    uow: unit_of_work.AbstractUnitOfWork,
    dts: data_sender.AbstractDataSender
):
    predictions: [DataFrame[TimeSeriesSchema]] = []
    with uow:
        locations: [model.Location] = uow.locations.get_all()
        mandatory_previous_receivers, sent_before = _query_params_for_impuls_predictions(
            cmd.send_even_if_not_sent_to_internal_fahrplanmanagement
        )

        for location in locations:
            if not _location_is_assigned_to_impuls(location):
                continue
            prediction = location.get_predicted_own_consumption(
                mandatory_previous_receivers=mandatory_previous_receivers,
                sent_before=sent_before,
            )
            if prediction is None:
                logger.error(f"Could not get valid own consumption prediction for location {location.alias}")
                continue
            prediction.rename(columns={"value": str(location.residual_long.id)}, inplace=True)
            predictions.append(prediction)
        for date, daily_df in _get_daily_dfs_from_predictions(predictions).items():
            dts.send_eigenverbrauch_to_impuls_energy_trading(daily_df, prediction_date=date)
        uow.commit()


def send_residual_long_predictions_to_impuls_energy_trading(
    cmd: commands.SendAllEigenverbrauchsPredictionsToImpuls,
    uow: unit_of_work.AbstractUnitOfWork,
    dts: data_sender.AbstractDataSender
):
    with uow:
        predictions = _get_predictions_for_impuls_energy_trading(
            uow,
            PredictionType.RESIDUAL_LONG,
            cmd.send_even_if_not_sent_to_internal_fahrplanmanagement,
        )

        for date, daily_df in _get_daily_dfs_from_predictions(predictions).items():
            dts.send_residual_long_to_impuls_energy_trading(daily_df, prediction_date=date)
        uow.commit()


def _location_is_assigned_to_impuls(location: model.Location) -> bool:
    return location.has_production and any(p.prognosis_data_retriever == DataRetriever.IMPULS_ENERGY_TRADING_SFTP for p in location.producers)


def _get_daily_dfs_from_predictions(
        predictions: [DataFrame[TimeSeriesSchema]]
) -> OrderedDict[datetime.date, DataFrame[IetLoadDataSchema]]:
    df = pd.concat(predictions, axis=1)
    df = df.tz_convert(TIMEZONE_UTC)
    df.index.name = "#timestamp"
    df = df.div(1000)  # convert from kW to MW
    df = df.round(3)  # todo clarify for which unit the 3 digits rule applies
    dfs_by_day = split_df_by_day(df, TIMEZONE_FILENAMES)

    daily_dfs = OrderedDict()
    for date in _dates_in_prognosis_horizon_impuls_energy_trading():
        daily_df = dfs_by_day.get(date)
        if daily_df is None:
            logger.error(f"Found no data for date {date} to send to Impuls Energy Trading")
            continue
        daily_df = DataFrame[IetLoadDataSchema](daily_df)
        daily_dfs[date] = daily_df
    return daily_dfs


def _dates_in_prognosis_horizon_impuls_energy_trading() -> [datetime.date]:
    dates_in_prognosis_horizon = []
    tomorrow = datetime.date.today() + datetime.timedelta(days=1)
    for n in range(6):
        dates_in_prognosis_horizon.append(tomorrow + datetime.timedelta(days=n))
    return dates_in_prognosis_horizon


def _get_predictions_for_impuls_energy_trading(
    uow: unit_of_work.AbstractUnitOfWork,
    prediction_type: PredictionType,
    send_even_if_not_sent_to_internal_fahrplanmanagement: bool = False,
) -> [DataFrame[TimeSeriesSchema]]:
    predictions: [DataFrame[TimeSeriesSchema]] = []
    locations: [model.Location] = uow.locations.get_all()
    for location in locations:
        if not _location_is_assigned_to_impuls(location):
            continue
        mandatory_previous_receivers, sent_before = _query_params_for_impuls_predictions(
            send_even_if_not_sent_to_internal_fahrplanmanagement
        )

        prediction = location.get_most_recent_prediction(
            prediction_type=prediction_type,
            receiver=mandatory_previous_receivers,
            sent_before=sent_before,
        )
        if prediction is None:
            logger.error(f"Could not get valid prediction for location {location.alias}")
            continue
        prediction.shipments.append(
            PredictionShipment(
                receiver=enums.PredictionReceiver.IMPULS_ENERGY_TRADING
            )
        )
        uow.locations.update(location)
        df = prediction.df.copy()
        TimeSeriesSchema.validate(df)
        df.columns = [str(location.residual_long.id)]
        predictions.append(df)
    return predictions


def _query_params_for_impuls_predictions(send_even_if_not_sent_to_internal_fahrplanmanagement: bool):
    if send_even_if_not_sent_to_internal_fahrplanmanagement:
        mandatory_previous_receivers = None
        sent_before = None
    else:
        mandatory_previous_receivers = enums.PredictionReceiver.INTERNAL_FAHRPLANMANAGEMENT
        sent_before = GATE_CLOSURE_INTERNAL_FAHRPLANMANAGEMENT
    return mandatory_previous_receivers, sent_before


def add_location(cmd: commands.CreateLocation, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        producers = []
        for p in cmd.producers:
            market_location = _build_market_location(
                id_=p["market_location_id"], number=p["market_location_number"], measurand=src.enums.Measurand.NEGATIVE,
            )
            producer = _build_producer(
                id_=p["id"], name=p["name"], market_location=market_location, prognosis_data_retriever=p["prognosis_data_retriever"]
            )
            producers.append(producer)

        residual_short = _build_market_location(
                id_=cmd.residual_short["id"], number=cmd.residual_short["number"], measurand=src.enums.Measurand.POSITIVE,
            )
        residual_long = _build_market_location(
                id_=cmd.residual_long["id"], number=cmd.residual_long["number"], measurand=src.enums.Measurand.NEGATIVE,
            ) if cmd.residual_long else None

        location = model.Location(
            state=cmd.state,
            alias=cmd.alias,
            tso=cmd.tso,
            residual_short=residual_short,
            residual_long=residual_long,
            producers=producers,
            settings=model.LocationSettings(
                active_from=cmd.settings_active_from,
                active_until=cmd.settings_active_until,
                send_consumption_predictions_to_fahrplanmanagement=cmd.settings_send_consumption_predictions_to_fahrplanmanagement,
                historic_days_for_consumption_prediction=cmd.settings_historic_days_for_consumption_prediction,
            ),
        )
        if cmd.id:
            location.id = cmd.id
        uow.locations.add(location)
        uow.commit()
        return location


def _build_market_location(id_: Optional[uuid.UUID], number: str, measurand: src.enums.Measurand) -> model.MarketLocation:
    malo = model.MarketLocation(
        number=number,
        measurand=measurand,
    )
    if id_:
        malo.id = id_
    return malo


def _build_producer(id_: Optional[uuid.UUID], name: str, market_location: model.MarketLocation, prognosis_data_retriever: src.enums.DataRetriever) -> model.Producer:
    producer = model.Producer(
        name=name,
        market_location=market_location,
        prognosis_data_retriever=prognosis_data_retriever
    )
    if id_:
        producer.id = id_
    return producer


def update_location_settings(
    cmd: commands.UpdateLocationSettings, uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        location: model.Location = uow.locations.get(UUID(cmd.location_id))

        location.settings = model.LocationSettings(
            active_from=cmd.settings_active_from,
            active_until=cmd.settings_active_until,
            send_consumption_predictions_to_fahrplanmanagement=cmd.settings_send_consumption_predictions_to_fahrplanmanagement,
            historic_days_for_consumption_prediction=cmd.settings_historic_days_for_consumption_prediction,
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
    commands.SendAllEigenverbrauchsPredictionsToImpuls: send_eigenverbrauchs_predictions_to_impuls_energy_trading,
    commands.SendAllResidualLongPredictionsToImpuls: send_residual_long_predictions_to_impuls_energy_trading,
}
