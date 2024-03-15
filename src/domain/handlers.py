from __future__ import annotations

import datetime
from uuid import uuid4
from src.domain import commands, events
from src.domain import model
from src.infrastructure import unit_of_work
from src.services import predictor, load_data, data_store


class InvalidRef(Exception):
    pass


def test_handler(event: events.CustomerCreated):
    print("TEST")


def add_project(cmd: commands.CreateProject, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        project = model.Project(id=uuid4(), name=cmd.name)
        uow.projects.add(project)
        uow.commit()
        return project


def add_location(cmd: commands.CreateLocation, uow: unit_of_work.AbstractUnitOfWork):
    with uow:
        company = uow.companies.get(cmd.company_ref)
        project = uow.projects.get(cmd.project_ref)
        location = model.Location(id=uuid4(), state=cmd.state, company=company, project=project)
        uow.locations.add(location)
        uow.commit()
        add_default_scope(events.LocationCreated(str(location.id)), uow)
        return location


def add_default_scope(
        evt: events.LocationCreated,
        uow: unit_of_work.AbstractUnitOfWork
):
    add_scope(commands.CreateScope(location_ref=evt.location_ref), uow)


def add_scope(
        cmd: commands.CreateScope,
        uow: unit_of_work.AbstractUnitOfWork
):
    pass


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
    cmd: commands.FetchLoadData,
    uow: unit_of_work.AbstractUnitOfWork,
    ldr: load_data.AbstractLoadDataRetriever,
):
    print("FETCHED DATA")
    components = uow.components.get_all()
    for component in components:
        try:
            energy_data = ldr.get_data(component.malo)
        except Exception as exc:
            pass


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
    with uow:
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
    dst.save_file(
        file_name=f"{prediction.component.malo}_{prediction.created}", buffer=buffer
    )


EVENT_HANDLERS = {
    events.CustomerCreated: [test_handler],
    events.LocationCreated: [add_default_scope],
    events.HistoricLoadProfileReceived: [create_prediction],
}

COMMAND_HANDLERS = {
    commands.CreateProject: add_project,
    commands.CreateLocation: add_location,
    commands.CreateCustomer: add_customer,
    commands.GetComponents: get_components,
    commands.CreateComponent: add_component,
    commands.AddHistoricLoadProfile: add_historic_load_profile,
    commands.FetchLoadData: fetch_load_data,
    commands.MakePrediction: make_prediction,
}
