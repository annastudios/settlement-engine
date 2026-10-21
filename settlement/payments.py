from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from settlement.models import Payment
from settlement.schemas import PaymentRequest, PaymentStatus

# The fields that make up the instruction itself (everything the client sent
# except the UETR). Two submissions with the same UETR must match on these.
_INSTRUCTION_FIELDS = (
    "rail",
    "amount",
    "currency",
    "debtor_name",
    "debtor_iban",
    "creditor_name",
    "creditor_iban",
    "remittance_info",
)


class UetrConflict(Exception):
    """The UETR already belongs to a payment with different details."""


def submit_payment(session: Session, request: PaymentRequest) -> tuple[Payment, bool]:
    """Store a new payment. Returns (payment, created).

    Submitting the same payment twice is safe: the second call returns the
    stored payment with created=False. Reusing a UETR for a different payment
    raises UetrConflict.

    INSERT ... ON CONFLICT DO NOTHING lets Postgres decide atomically, so this
    holds even when two copies arrive at the same moment. "Check if it exists,
    then insert" would not: both requests could pass the check before either
    inserts.
    """
    values = request.model_dump()
    values["rail"] = request.rail.value
    values["status"] = PaymentStatus.RECEIVED.value

    inserted_uetr = session.execute(
        insert(Payment)
        .values(**values)
        .on_conflict_do_nothing(index_elements=[Payment.uetr])
        .returning(Payment.uetr)
    ).scalar_one_or_none()
    session.commit()

    payment = session.get(Payment, request.uetr)
    created = inserted_uetr is not None
    if not created and not _same_instruction(payment, request):
        raise UetrConflict(
            f"UETR {request.uetr} is already used by a different payment"
        )
    return payment, created


def _same_instruction(payment: Payment, request: PaymentRequest) -> bool:
    return all(
        getattr(payment, field) == getattr(request, field)
        for field in _INSTRUCTION_FIELDS
    )
