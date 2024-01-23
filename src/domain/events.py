from uuid import UUID
from dataclasses import dataclass


class Event:
    pass


@dataclass
class CustomerCreated(Event):
    customer_id: UUID
