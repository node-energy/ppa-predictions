from __future__ import annotations
from uuid import uuid4
from src.domain import commands, events
from src.domain import model
from src.infrastructure import unit_of_work
from src.services import predictor


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
        component = model.Component(ref=uuid4(), type=cmd.type, location=location)
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
        uow: unit_of_work.AbstractUnitOfWork
):
    pass


def make_prediction(
        cmd: commands.MakePrediction,
        uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        component = uow.components.get(cmd.component_ref)
        if component is None:
            raise Exception  # raise InvlaidComponentID




EVENT_HANDLERS = {
    events.CustomerCreated: [test_handler],
}

COMMAND_HANDLERS = {
    commands.CreateCustomer: add_customer,
    commands.GetComponents: get_components,
    commands.CreateComponent: add_component,
    commands.AddHistoricLoadProfile: add_historic_load_profile,
    commands.FetchLoadData: fetch_load_data,
    commands.MakePrediction: make_prediction,
}
