import uuid
from decimal import Decimal

import httpx
import pytest

from settlement import rail_client
from settlement.models import Payment
from settlement.rail_client import RailRejected, RailUnavailable, send_to_rail


def payment(creditor_iban="GB82WEST12345698765432"):
    return Payment(
        uetr=uuid.uuid4(),
        rail="FPS",
        amount=Decimal("250.00"),
        currency="GBP",
        debtor_iban="GB33BUKB20201555555555",
        creditor_iban=creditor_iban,
    )


def use_fake_rail(monkeypatch, handler):
    """Replace the rail with a function that answers (or fails) each request."""
    fake = httpx.Client(transport=httpx.MockTransport(handler), base_url="http://rail")
    monkeypatch.setattr(rail_client, "_client", fake)


def test_settled_payment_returns_normally(rail):
    p = payment()

    send_to_rail(p)

    assert rail.get(f"/payments/{p.uetr}").json()["status"] == "settled"


def test_rejection_raises_with_the_reason_code():
    with pytest.raises(RailRejected) as exc_info:
        send_to_rail(payment(creditor_iban="GB56CLOS00000000000001"))

    assert exc_info.value.reason == "AC04"


def test_503_is_unavailable(rail):
    rail.put("/chaos", json={"failure_rate": 1})

    with pytest.raises(RailUnavailable):
        send_to_rail(payment())


def test_timeout_is_unavailable(monkeypatch):
    def too_slow(request):
        raise httpx.ReadTimeout("rail took too long", request=request)

    use_fake_rail(monkeypatch, too_slow)

    with pytest.raises(RailUnavailable):
        send_to_rail(payment())


def test_refused_connection_is_unavailable(monkeypatch):
    def nobody_there(request):
        raise httpx.ConnectError("connection refused", request=request)

    use_fake_rail(monkeypatch, nobody_there)

    with pytest.raises(RailUnavailable):
        send_to_rail(payment())


def test_our_own_bad_request_is_a_bug_not_an_outage(monkeypatch):
    use_fake_rail(monkeypatch, lambda request: httpx.Response(422, json={"detail": "bad"}))

    with pytest.raises(httpx.HTTPStatusError):
        send_to_rail(payment())


def test_resending_after_a_lost_answer_moves_the_money_once(rail):
    # Why "unavailable" must be resent with the SAME UETR: the first attempt
    # settled, but its answer was lost.
    p = payment()
    rail.put("/chaos", json={"drop_response_rate": 1, "hang_seconds": 0})
    with pytest.raises(RailUnavailable):
        send_to_rail(p)

    rail.put("/chaos", json={})
    send_to_rail(p)  # the resend: settled, no error

    ledger = rail.get(f"/payments/{p.uetr}").json()
    assert ledger["status"] == "settled"
    assert ledger["received_count"] == 2  # arrived twice, money moved once
