from __future__ import annotations

import datetime
from uuid import uuid4
from src.domain import commands, events
from src.domain import model
from src.infrastructure import unit_of_work
from src.services import predictor, load_data, data_store


def test_handler(event: events.CustomerCreated):
    print("TEST")


def add_customer(
        cmd: commands.CreateCustomer
):
    customer = model.Customer(ref=uuid4())
    return customer


def get_components(
        cmd: commands.GetComponents,
        uow: unit_of_work.AbstractUnitOfWork
):
    components = uow.components.get_all()
    return components


def add_component(
        cmd: commands.CreateComponent,
        uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        location = uow.locations.get(cmd.location_ref)
        component = model.Component(ref=uuid4(), malo=cmd.malo, type=cmd.type, location=location)
        uow.components.add(component)
        uow.commit()
    return component


def add_historic_load_profile(
        cmd: commands.AddHistoricLoadProfile,
        uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        component = uow.components.get(cmd.component_ref)
        if component is None:
            raise Exception()  # raise InvalidComponentID

        hlp = model.HistoricLoadProfile(ref=uuid4(), component=component, timestamps=cmd.timestamps)
        uow.historic_load_profiles.add(hlp)
        uow.commit()


def fetch_load_data(
        cmd: commands.FetchLoadData,
        uow: unit_of_work.AbstractUnitOfWork,
        ldr: load_data.AbstractLoadDataRetriever
):
    print("FETCHED DATA")
    components = uow.components.get_all()
    for component in components:
        try:
            energy_data = ldr.get_data(component.malo)
        except Exception as exc:
            pass


def make_prediction(
        cmd: commands.MakePrediction,
        uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        component = uow.components.get(cmd.component_ref)
        if component is None:
            raise Exception  # raise InvalidComponentID


def create_prediction(
        evt: events.HistoricLoadProfileReceived,
        uow: unit_of_work.AbstractUnitOfWork,
        dst: data_store.AbstractDataStore
):
    with uow:
        component = uow.components.get(evt.component_ref)
        load_profile = uow.historic_load_profiles.get_by_component_ref(component.ref).to_dataframe()  # TODO to slice
        predictr = predictor.RandomForestPredictor()
        predictr.configure(historic_load_profile_slice=load_profile, state=component.location.state)
        prediction_df = predictr.create_prediction()
        prediction = model.Prediction.from_dataframe(uuid4(), component, datetime.datetime.now(), prediction_df)
        uow.predictions.add(prediction)
        store_prediction_file(events.PredictionCreated(prediction.ref), uow, dst)


def store_prediction_file(
        evt: events.PredictionCreated,
        uow: unit_of_work.AbstractUnitOfWork,
        dst: data_store.AbstractDataStore
):
    prediction = uow.predictions.get(evt.prediction_ref)
    buffer = prediction.to_csv_buffer()
    dst.save_file(file_name=f"{prediction.component.malo}_{prediction.created}", buffer=buffer)


EVENT_HANDLERS = {
    events.CustomerCreated: [test_handler],
    events.HistoricLoadProfileReceived: [create_prediction]
}

COMMAND_HANDLERS = {
    commands.CreateCustomer: add_customer,
    commands.GetComponents: get_components,
    commands.CreateComponent: add_component,
    commands.AddHistoricLoadProfile: add_historic_load_profile,
    commands.FetchLoadData: fetch_load_data,
    commands.MakePrediction: make_prediction,
}
