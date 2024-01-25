from uuid import uuid4
from src.domain.model import Customer


def test_create_customer():
    ref = uuid4()
    customer = Customer(ref=ref)
    assert customer.ref == ref
