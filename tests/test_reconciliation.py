from datetime import UTC, datetime, timedelta

import httpx
import pytest

from settlement import config, rail_client, reconciliation, worker
from settlement.connections import SessionLocal
from settlement.connections import redis_client as r
from settlement.reconciliation import reconcile
from tests.factories import fps_payment, payment_status, sepa_payment


def ours(uetr, status, amount="100.00", currency="GBP"):
    return {"uetr": uetr, "status": status, "amount": amount, "currency": currency}


def theirs(uetr, status, amount="100.00", currency="GBP", received_count=1):
    return {
        "uetr": uetr, "status": status, "amount": amount, "currency": currency,
        "received_count": received_count,
    }


# --- comparing the two sides ---------------------------------------------------


def test_everything_agrees():
    report = reconcile(
        [ours("a", "settled"), ours("b", "rejected"), ours("c", "settled", "50.00", "EUR")],
        [theirs("a", "settled"), theirs("b", "rejected"), theirs("c", "settled", "50.00", "EUR")],
    )

    assert report["balanced"] is True
    assert report["matched"] == 3
    assert report["breaks"] == []
    assert report["settled_totals"] == {
        "ours": {"EUR": {"count": 1, "amount": "50.00"}, "GBP": {"count": 1, "amount": "100.00"}},
        "network": {"EUR": {"count": 1, "amount": "50.00"}, "GBP": {"count": 1, "amount": "100.00"}},
    }


def test_unfinished_payments_the_network_has_not_seen_are_pending_not_breaks():
    report = reconcile(
        [ours("a", "received"), ours("b", "retrying"), ours("c", "in_repair")], []
    )

    assert report["balanced"] is True
    assert report["pending"] == {"received": 1, "retrying": 1, "in_repair": 1}


@pytest.mark.parametrize(
    "our_record, network_record, kind",
    [
        # The money moved but we think it didn't (e.g. a lost answer that
        # ended in the repair queue).
        (ours("x", "in_repair"), theirs("x", "settled"), "network_settled_we_did_not"),
        (ours("x", "rejected"), theirs("x", "settled"), "network_settled_we_did_not"),
        (ours("x", "settled"), theirs("x", "rejected"), "network_rejected_we_did_not"),
        (ours("x", "settled"), None, "missing_at_network"),
        (ours("x", "rejected"), None, "missing_at_network"),
        (None, theirs("x", "settled"), "unknown_to_us"),
        (ours("x", "settled", "100.00"), theirs("x", "settled", "100.01"), "amount_mismatch"),
        (ours("x", "settled", currency="GBP"), theirs("x", "settled", currency="EUR"), "amount_mismatch"),
    ],
)
def test_each_kind_of_break(our_record, network_record, kind):
    report = reconcile(
        [our_record] if our_record else [], [network_record] if network_record else []
    )

    assert report["balanced"] is False
    [found] = report["breaks"]
    assert found["kind"] == kind
    assert found["uetr"] == "x"
    assert found["explanation"]


def test_amounts_compare_as_money_not_text():
    report = reconcile([ours("a", "settled", "100")], [theirs("a", "settled", "100.00")])

    assert report["balanced"] is True


def test_resends_the_network_absorbed_are_counted():
    report = reconcile(
        [ours("a", "settled"), ours("b", "settled")],
        [theirs("a", "settled", received_count=3), theirs("b", "settled")],
    )

    assert report["resends_absorbed"] == 2
    assert report["balanced"] is True


# --- a real day, end to end ----------------------------------------------------


def today():
    return datetime.now(UTC).date()


def submit(client, payment):
    assert client.post("/payments", json=payment).status_code == 201
    return payment["uetr"]


def drain():
    while worker.process_one(r, "worker-a") is not None:
        pass


def test_a_normal_day_balances(client):
    for payment in [fps_payment(), fps_payment(), sepa_payment(),
                    fps_payment(creditor_iban="GB56CLOS00000000000001")]:
        submit(client, payment)
    drain()

    with SessionLocal() as session:
        report = reconciliation.run_for_day(session, today())

    assert report["balanced"] is True
    assert report["matched"] == 4
    assert report["settled_totals"]["ours"] == report["settled_totals"]["network"]
    assert report["settled_totals"]["ours"]["GBP"] == {"count": 2, "amount": "500.00"}


def test_a_lost_answer_left_in_repair_is_caught_and_fixed(client, rail, monkeypatch):
    # Every attempt settles at the network but the answer is lost, so we give
    # up and put it in the repair queue: we think it failed, the money moved.
    monkeypatch.setattr(config, "RETRY_BASE_SECONDS", 0)
    monkeypatch.setattr(config, "MAX_ATTEMPTS", 2)
    uetr = submit(client, fps_payment())
    rail.put("/chaos", json={"drop_response_rate": 1, "hang_seconds": 0})
    drain()
    assert payment_status(uetr) == "in_repair"

    report = client.get("/reconciliation").json()

    assert report["balanced"] is False
    [found] = report["breaks"]
    assert found["kind"] == "network_settled_we_did_not"
    assert (found["our_status"], found["network_status"]) == ("in_repair", "settled")

    # The fix: resubmit. Same UETR, so the network confirms instead of paying again.
    rail.put("/chaos", json={})
    client.post(f"/payments/{uetr}/resubmit")
    drain()

    report = client.get("/reconciliation").json()
    assert report["balanced"] is True
    assert report["resends_absorbed"] == 2  # 3 copies arrived, money moved once


def test_another_day_has_nothing_to_reconcile(client):
    submit(client, fps_payment())
    drain()

    report = client.get(f"/reconciliation?day={today() - timedelta(days=1)}").json()

    assert report["balanced"] is True
    assert report["matched"] == 0


def test_reconciliation_needs_the_network(client, monkeypatch):
    def nobody_there(request):
        raise httpx.ConnectError("refused", request=request)

    fake = httpx.Client(transport=httpx.MockTransport(nobody_there), base_url="http://rail")
    monkeypatch.setattr(rail_client, "_client", fake)

    assert client.get("/reconciliation").status_code == 503
