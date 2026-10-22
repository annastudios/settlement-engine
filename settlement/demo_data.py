"""Random but valid test payments, for the dashboard's buttons and the demo."""

import random
import uuid
from decimal import Decimal

from settlement.schemas import PaymentRequest

_DEBTORS = ["Alice Smith", "Ravi Patel", "Chen Wei", "Maria Garcia", "Tom Brown"]
_CREDITORS = ["Bob's Bakery Ltd", "Northwind Energy", "City Council Tax", "Acme Rentals"]
_DEBTOR_IBAN = "GB33BUKB20201555555555"
_CREDITOR_IBAN = {"FPS": "GB82WEST12345698765432", "SEPA": "DE89370400440532013000"}
CLOSED_ACCOUNT = "GB56CLOS00000000000001"  # the rail rejects it with AC04


def random_payment(closed_account_chance: float = 0.1) -> PaymentRequest:
    """A random FPS or SEPA payment. FPS ones go to a closed account (which
    the rail rejects) with the given chance."""
    rail = random.choice(["FPS", "FPS", "SEPA"])
    creditor_iban = _CREDITOR_IBAN[rail]
    if rail == "FPS" and random.random() < closed_account_chance:
        creditor_iban = CLOSED_ACCOUNT
    return PaymentRequest(
        uetr=uuid.uuid4(),
        rail=rail,
        amount=Decimal(random.randint(100, 500_000)) / 100,
        currency="GBP" if rail == "FPS" else "EUR",
        debtor_name=random.choice(_DEBTORS),
        debtor_iban=_DEBTOR_IBAN,
        creditor_name=random.choice(_CREDITORS),
        creditor_iban=creditor_iban,
        remittance_info=f"Demo {random.randint(1000, 9999)}",
    )
