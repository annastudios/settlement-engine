import uuid
from datetime import datetime
from decimal import Decimal

from sqlalchemy import CheckConstraint, DateTime, Numeric, String, Uuid, func
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
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now()
    )

    # The API validates this too; the database is the last line of defence.
    __table_args__ = (CheckConstraint("amount > 0", name="amount_positive"),)
