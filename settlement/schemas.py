import uuid
from datetime import datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from settlement.iban import normalize_iban


class Rail(StrEnum):
    FPS = "FPS"  # UK Faster Payments
    SEPA = "SEPA"  # SEPA Credit Transfer (euro area)


class PaymentStatus(StrEnum):
    RECEIVED = "received"  # stored and queued, waiting for a worker
    PROCESSING = "processing"  # a worker is sending it to the rail
    RETRYING = "retrying"  # the rail was unavailable; waiting for the next attempt
    SETTLED = "settled"  # the rail accepted it: money moved
    REJECTED = "rejected"  # the rail refused it for good; see rejection_reason
    IN_REPAIR = "in_repair"  # out of attempts; needs a person (the repair queue)


# Each rail settles in exactly one currency.
RAIL_CURRENCY = {Rail.FPS: "GBP", Rail.SEPA: "EUR"}
# Faster Payments scheme limit for a single payment.
FPS_MAX_AMOUNT = Decimal("1000000.00")


class PaymentRequest(BaseModel):
    """A payment instruction, loosely modelled on ISO 20022 pacs.008.

    Text limits (140 chars) follow ISO 20022's Max140Text.
    """

    model_config = ConfigDict(extra="forbid")

    uetr: uuid.UUID
    rail: Rail
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2)
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    debtor_name: str = Field(min_length=1, max_length=140)
    debtor_iban: str
    creditor_name: str = Field(min_length=1, max_length=140)
    creditor_iban: str
    remittance_info: str | None = Field(default=None, max_length=140)

    @field_validator("debtor_iban", "creditor_iban")
    @classmethod
    def _valid_iban(cls, value: str) -> str:
        return normalize_iban(value)

    @model_validator(mode="after")
    def _rail_rules(self):
        expected = RAIL_CURRENCY[self.rail]
        if self.currency != expected:
            raise ValueError(f"{self.rail} payments must be in {expected}")
        if self.rail is Rail.FPS:
            if self.amount > FPS_MAX_AMOUNT:
                raise ValueError(f"FPS payments are limited to {FPS_MAX_AMOUNT} GBP")
            if not self.creditor_iban.startswith("GB"):
                raise ValueError("FPS creditor account must be a UK (GB) IBAN")
        return self


class PaymentResponse(PaymentRequest):
    model_config = ConfigDict(from_attributes=True)

    status: PaymentStatus
    # ISO 20022 reason code from the rail, e.g. AC04 = closed account.
    rejection_reason: str | None = None
    attempts: int
    last_error: str | None = None
    created_at: datetime
