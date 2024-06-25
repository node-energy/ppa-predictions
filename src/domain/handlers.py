from __future__ import annotations

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
        location: model.Location = uow.locations.get(
            UUID(cmd.location_id)
        )

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
        location: model.Location = uow.locations.get(
            UUID(cmd.location_id)
        )

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
    uow: unit_of_work.AbstractUnitOfWork
):
    if settings.send_predictions_enabled:
        pass


def add_location(cmd: commands.CreateLocation, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        location = model.Location(
            state=cmd.state, residual_short=model.Consumer(malo=cmd.residual_short_malo)
        )
        uow.locations.add(location)
        uow.commit()
        return location


def add_customer(cmd: commands.CreateCustomer):
    customer = model.Customer(id=uuid4())
    return customer


def get_components(cmd: commands.GetComponents, uow: unit_of_work.AbstractUnitOfWork):
    components = uow.components.get_all()
    return components


def add_component(cmd: commands.CreateComponent, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        location = uow.locations.get(cmd.location_ref)
        component = model.Component(
            id=uuid4(), malo=cmd.malo, type=cmd.type, location=location
        )
        uow.components.add(component)
        uow.commit()
    return component


def add_historic_load_profile(
    cmd: commands.AddHistoricLoadProfile, uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        component = uow.components.get(cmd.component_ref)
        if component is None:
            raise InvalidRef()

        hlp = model.HistoricLoadProfile(
            id=uuid4(), component=component, timestamps=cmd.timestamps
        )
        uow.historic_load_profiles.add(hlp)
        uow.commit()


def fetch_load_data(
    _: commands.FetchLoadData,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: load_data.AbstractLoadDataRetriever,
):
    print("FETCHED DATA")
    components: list[model.Component] = uow.components.get_all()
    for component in components:  # TODO big loop
        try:
            energy_data = ldr.get_data(component.malo)
            hlp = model.HistoricLoadProfile.from_dataframe(
                uuid4(), component, energy_data
            )
            with uow:
                uow.historic_load_profiles.add(
                    hlp
                )  # TODO only add if not existing, aggregate?
        except Exception as exc:
            logger.error(exc)
            continue


def make_prediction(cmd: commands.MakePrediction, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        component = uow.components.get(cmd.component_ref)
        if component is None:
            raise Exception  # raise InvalidComponentID


def create_prediction(
    evt: events.HistoricLoadProfileReceived,
    uow: unit_of_work.AbstractUnitOfWork,
    dst: data_store.AbstractDataStore,
):
    with uow:  # Should be part of domain model
        component = uow.components.get(evt.component_ref)
        load_profile = uow.historic_load_profiles.get_by_component_ref(
            component.id
        ).to_dataframe()  # TODO to slice
        predictr = predictor.RandomForestPredictor()
        predictr.configure(
            historic_load_profile_slice=load_profile, state=component.location.state
        )
        prediction_df = predictr.create_prediction()
        prediction = model.Prediction.from_dataframe(
            uuid4(), component, datetime.datetime.now(), prediction_df
        )
        uow.predictions.add(prediction)
        store_prediction_file(events.PredictionCreated(prediction.id), uow, dst)


def store_prediction_file(
    evt: events.PredictionCreated,
    uow: unit_of_work.AbstractUnitOfWork,
    dst: data_store.AbstractDataStore,
):
    prediction = uow.predictions.get(evt.prediction_ref)
    buffer = prediction.to_csv_buffer()
    recipient = (
        settings.recipient_production
        if prediction.component.type == "producer"
        else settings.recipient_consumption
    )
    dst.save_file(
        file_name=f"{prediction.component.malo}_{prediction.created}",
        buffer=buffer,
        recipient=recipient,
    )


# new
def fetch_all_historic_data(
    _: commands.FetchAllHistoricData,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: load_data.AbstractLoadDataRetriever,
):
    logger.info("Start fetching data for all components")
    with uow:
        components: list[model.Component] = uow.components.get_all()
        for component in components:
            fetch_historic_data_for_component(
                commands.FetchHistoricDataForComponent(component_id=str(component.id)),
                uow,
                ldr,
            )
    logger.info("Finished fetching data for all components")


def fetch_historic_data_for_component(
    cmd: commands.FetchHistoricDataForComponent,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: load_data.AbstractLoadDataRetriever,
):
    with uow:
        component = uow.components.get(cmd.component_id)
        historic_df = ldr.get_data(component.malo)
        historic_load_profile = model.HistoricLoadProfile.from_dataframe(
            uuid4(), component, historic_df
        )
        uow.historic_load_profiles.add(historic_load_profile)
        uow.commit()


def make_all_predictions(
    _: commands.MakeAllPredictions,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: load_data.AbstractLoadDataRetriever,
):
    logger.info("Start making predictions for all components")
    with uow:
        components: list[model.Component] = uow.components.get_all()
        for component in components:
            component.predict()

            fetch_historic_data_for_component(
                commands.FetchHistoricDataForComponent(component_id=str(component.id)),
                uow,
                ldr,
            )
    logger.info("Finished making predictions for all components")


EVENT_HANDLERS = {
    events.CustomerCreated: [test_handler],
    events.HistoricLoadProfileReceived: [create_prediction],
}

COMMAND_HANDLERS = {
    commands.CreateLocation: add_location,
    commands.GetComponents: get_components,
    commands.CreateComponent: add_component,
    commands.FetchLoadData: fetch_load_data,
    commands.MakePrediction: make_prediction,
    # new
    commands.FetchAllHistoricData: fetch_all_historic_data,
    commands.FetchHistoricDataForComponent: fetch_historic_data_for_component,
    commands.MakeAllPredictions: make_all_predictions,
    commands.UpdateHistoricData: update_historic_data,
    commands.CalculatePredictions: calculate_predictions,
}
