"""Every change to a payment goes through here, so it is always recorded in
the audit trail (the payment_events table) in the same transaction.

Either the change and its audit row are both saved, or neither is: the
history can never miss a change, or describe one that didn't happen.
"""

import uuid

from sqlalchemy import select, update
from sqlalchemy.orm import Session

from settlement.models import Payment, PaymentEvent


def move(
    session: Session,
    uetr: uuid.UUID,
    from_statuses: tuple[str, ...],
    to_status: str,
    *,
    actor: str,
    reason: str | None = None,
    only_if: tuple = (),
    **changes,
) -> bool:
    """Change the payment's status (and any columns in changes) only if it is
    currently one of from_statuses and meets every condition in only_if, and
    record the change. Commits. True if it changed.

    The row is locked (SELECT ... FOR UPDATE) while it is checked and changed,
    so two workers trying to move the same payment at once can't both
    succeed: the second waits, sees the new status and gets False.
    """
    from_status = session.scalar(
        select(Payment.status)
        .where(Payment.uetr == uetr, Payment.status.in_(from_statuses), *only_if)
        .with_for_update()
    )
    if from_status is None:
        session.rollback()
        return False

    attempt = session.scalar(
        update(Payment)
        .where(Payment.uetr == uetr)
        .values(status=to_status, **changes)
        .returning(Payment.attempts)
    )
    session.add(
        PaymentEvent(
            uetr=uetr, from_status=from_status, to_status=to_status,
            attempt=attempt, actor=actor, reason=reason,
        )
    )
    session.commit()
    return True


def note(session: Session, uetr: uuid.UUID, *, actor: str, reason: str) -> None:
    """Record something that happened to a payment without changing its
    status (e.g. recovery putting it back in the queue). Commits."""
    status, attempt = session.execute(
        select(Payment.status, Payment.attempts).where(Payment.uetr == uetr)
    ).one()
    session.add(
        PaymentEvent(
            uetr=uetr, from_status=status, to_status=status,
            attempt=attempt, actor=actor, reason=reason,
        )
    )
    session.commit()


def history(session: Session, uetr: uuid.UUID) -> list[PaymentEvent]:
    """A payment's audit trail, oldest first."""
    return list(
        session.scalars(
            select(PaymentEvent).where(PaymentEvent.uetr == uetr).order_by(PaymentEvent.id)
        )
    )
