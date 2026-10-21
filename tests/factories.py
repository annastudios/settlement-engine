import uuid

from sqlalchemy import func, select

from settlement.connections import SessionLocal
from settlement.models import Payment


def fps_payment(**overrides):
    """A valid FPS payment with a fresh UETR. Pass overrides to change fields."""
    payment = {
        "uetr": str(uuid.uuid4()),
        "rail": "FPS",
        "amount": "250.00",
        "currency": "GBP",
        "debtor_name": "Alice Smith",
        "debtor_iban": "GB33BUKB20201555555555",
        "creditor_name": "Bob's Bakery Ltd",
        "creditor_iban": "GB82WEST12345698765432",
        "remittance_info": "Invoice 1042",
    }
    return payment | overrides


def sepa_payment(**overrides):
    return fps_payment(
        rail="SEPA",
        currency="EUR",
        creditor_iban="DE89370400440532013000",
        **overrides,
    )


def count_payments():
    with SessionLocal() as session:
        return session.scalar(select(func.count()).select_from(Payment))


def payment_status(uetr):
    with SessionLocal() as session:
        return session.get(Payment, uuid.UUID(uetr)).status
