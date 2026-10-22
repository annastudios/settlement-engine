import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import (
    DDL,
    BigInteger,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Integer,
    Numeric,
    String,
    Text,
    Uuid,
    event,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Payment(Base):
    __tablename__ = "payments"

    # UETR: the unique end-to-end reference every payment carries (SWIFT gpi,
    # ISO 20022). As the primary key it is also the idempotency key: Postgres
    # itself guarantees there is never more than one row per payment.
    uetr: Mapped[uuid.UUID] = mapped_column(Uuid, primary_key=True)
    rail: Mapped[str] = mapped_column(String(8))
    # Money is NUMERIC, never float: in floating point 0.1 + 0.2 != 0.3.
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[str] = mapped_column(String(3))
    debtor_name: Mapped[str] = mapped_column(String(140))
    debtor_iban: Mapped[str] = mapped_column(String(34))
    creditor_name: Mapped[str] = mapped_column(String(140))
    creditor_iban: Mapped[str] = mapped_column(String(34))
    remittance_info: Mapped[str | None] = mapped_column(String(140))
    status: Mapped[str] = mapped_column(String(16))
    # ISO 20022 reason code when the rail rejects the payment (e.g. AC04).
    rejection_reason: Mapped[str | None] = mapped_column(String(4))
    # How many times this payment has been sent to the rail.
    attempts: Mapped[int] = mapped_column(Integer, default=0, server_default="0")
    # Why the last attempt failed, for whoever looks at the repair queue.
    last_error: Mapped[str | None] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )
    # Set on every change. Recovery uses it to tell a payment that is quietly
    # stuck from one that was touched a moment ago.
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )

    # The API validates this too; the database is the last line of defence.
    __table_args__ = (CheckConstraint("amount > 0", name="amount_positive"),)


class PaymentEvent(Base):
    """The audit trail: one row per change to a payment, never edited or
    deleted. Written in the same transaction as the change it describes, so
    the two can never disagree (see settlement.audit)."""

    __tablename__ = "payment_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    uetr: Mapped[uuid.UUID] = mapped_column(ForeignKey("payments.uetr"), index=True)
    # clock_timestamp(), not now(): now() is the transaction's start time, so
    # two events in one transaction would get the same time.
    at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.clock_timestamp()
    )
    # None for the first event (the payment didn't exist before it).
    from_status: Mapped[str | None] = mapped_column(String(16))
    to_status: Mapped[str] = mapped_column(String(16))
    # The payment's attempt count after this change.
    attempt: Mapped[int] = mapped_column(Integer)
    # Who made the change: "api", "worker <id>", "recovery by worker <id>", "ops".
    actor: Mapped[str] = mapped_column(String(120))
    reason: Mapped[str | None] = mapped_column(Text)


# Append-only, enforced by Postgres itself: an UPDATE or DELETE on the audit
# trail fails even for code (or a person) that bypasses this application.
event.listen(
    PaymentEvent.__table__,
    "after_create",
    DDL(
        """
        CREATE OR REPLACE FUNCTION payment_events_are_append_only() RETURNS trigger AS $$
        BEGIN
            RAISE EXCEPTION 'payment_events is append-only: %% is not allowed', TG_OP;
        END;
        $$ LANGUAGE plpgsql
        """
    ),
)
event.listen(
    PaymentEvent.__table__,
    "after_create",
    DDL(
        """
        CREATE TRIGGER payment_events_append_only
        BEFORE UPDATE OR DELETE ON payment_events
        FOR EACH ROW EXECUTE FUNCTION payment_events_are_append_only()
        """
    ),
)
