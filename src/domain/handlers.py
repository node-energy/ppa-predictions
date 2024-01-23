from __future__ import annotations
from uuid import uuid4
from src.domain import commands, events
from src.domain.model import Customer, Component
from src.infrastructure import unit_of_work


def test_handler(event: events.CustomerCreated):
    print("TEST")


def add_customer(
        cmd: commands.CreateCustomer
):
    customer = Customer(id=uuid4())
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
        component = Component(ref=uuid4(), type=cmd.type)
        uow.components.add(component)
        uow.commit()
    return component


def add_historic_load_profile(
        cmd: commands.AddHistoricLoadProfile,
        uow: unit_of_work.AbstractUnitOfWork
):
    with uow:
        component = uow.components.get(cmd.component_id)
        if component is None:
            raise Exception()  # raise InvalidComponentID
        component.add_historic_load_profile(cmd.timestamps)
        uow.commit()


EVENT_HANDLERS = {
    events.CustomerCreated: [test_handler],
}

COMMAND_HANDLERS = {
    commands.CreateCustomer: add_customer,
    commands.GetComponents: get_components,
    commands.CreateComponent: add_component,
    commands.AddHistoricLoadProfile: add_historic_load_profile,
}