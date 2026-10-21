"""Sends payments to the rail and turns its answer into one of three outcomes:

  settled      send_to_rail returns normally
  rejected     RailRejected: a final "no" (e.g. closed account). Never resend.
  unavailable  RailUnavailable: the outcome is unknown. Resend with the SAME
               UETR, never as a new payment: the rail dedupes on the UETR, so
               a resend cannot move the money twice.
"""

import httpx

from settlement import config
from settlement.models import Payment

_client = httpx.Client(base_url=config.RAIL_URL, timeout=config.RAIL_TIMEOUT_SECONDS)


class RailRejected(Exception):
    def __init__(self, reason: str):
        super().__init__(f"rejected by the rail: {reason}")
        self.reason = reason


class RailUnavailable(Exception):
    """The rail failed or did not answer in time. The payment may or may not
    have gone through."""


def send_to_rail(payment: Payment) -> None:
    body = {
        "uetr": str(payment.uetr),
        "rail": payment.rail,
        "amount": str(payment.amount),
        "currency": payment.currency,
        "debtor_iban": payment.debtor_iban,
        "creditor_iban": payment.creditor_iban,
    }
    try:
        response = _client.post("/payments", json=body)
    except httpx.TransportError as exc:  # timeouts, refused connections, resets
        raise RailUnavailable(f"no answer from the rail: {exc!r}") from exc
    if response.status_code >= 500:
        raise RailUnavailable(f"rail answered {response.status_code}")
    # Any other error status is a bug on our side, not a rail outage.
    response.raise_for_status()

    answer = response.json()
    if answer["status"] == "rejected":
        raise RailRejected(answer["reason"])


def get_chaos() -> dict:
    """The rail simulator's current chaos settings (for the dashboard)."""
    response = _client.get("/chaos")
    response.raise_for_status()
    return response.json()


def set_chaos(settings: dict) -> dict:
    """Change the rail simulator's chaos settings (for the dashboard)."""
    response = _client.put("/chaos", json=settings)
    response.raise_for_status()
    return response.json()
