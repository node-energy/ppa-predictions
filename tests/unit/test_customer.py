from uuid import uuid4
from src.domain.model import Customer


def test_create_customer():
    id = uuid4()
    customer = Customer(id=id)
    assert customer.id == id
