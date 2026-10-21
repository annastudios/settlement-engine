import threading
import uuid
from concurrent.futures import ThreadPoolExecutor

import pytest

from settlement import connections, work_queue
from settlement.connections import SessionLocal, redis_client
from settlement.payments import submit_payment
from settlement.schemas import PaymentRequest
from tests.factories import count_payments, dead_redis, fps_payment, sepa_payment


def test_submit_fps_payment(client):
    payment = fps_payment()

    response = client.post("/payments", json=payment)

    assert response.status_code == 201
    body = response.json()
    assert body["uetr"] == payment["uetr"]
    assert body["amount"] == "250.00"  # money comes back as a string, never a float
    assert body["status"] == "received"
    assert "created_at" in body


def test_submit_sepa_payment(client):
    response = client.post("/payments", json=sepa_payment())

    assert response.status_code == 201
    assert response.json()["currency"] == "EUR"


def test_iban_is_stored_normalised(client):
    payment = fps_payment(creditor_iban="gb82 west 1234 5698 7654 32")

    response = client.post("/payments", json=payment)

    assert response.json()["creditor_iban"] == "GB82WEST12345698765432"


def test_get_payment(client):
    payment = fps_payment()
    client.post("/payments", json=payment)

    response = client.get(f"/payments/{payment['uetr']}")

    assert response.status_code == 200
    assert response.json()["creditor_name"] == "Bob's Bakery Ltd"


def test_get_unknown_payment_is_404(client):
    response = client.get(f"/payments/{uuid.uuid4()}")

    assert response.status_code == 404


def test_same_payment_twice_is_stored_once(client):
    payment = fps_payment()

    first = client.post("/payments", json=payment)
    second = client.post("/payments", json=payment)

    assert first.status_code == 201
    assert second.status_code == 200
    assert second.json() == first.json()
    assert count_payments() == 1
    # Queued once only: the repeat must not create a second job.
    assert redis_client.lrange(work_queue.READY, 0, -1) == [payment["uetr"]]


def test_new_payment_is_queued_for_processing(client):
    payment = fps_payment()

    client.post("/payments", json=payment)

    assert redis_client.lrange(work_queue.READY, 0, -1) == [payment["uetr"]]


def test_payment_is_accepted_even_if_redis_is_down(client, monkeypatch):
    # Postgres has it, so the client must get 201, not an error.
    monkeypatch.setattr(connections, "redis_client", dead_redis())
    payment = fps_payment()

    response = client.post("/payments", json=payment)

    assert response.status_code == 201
    assert count_payments() == 1


def test_same_amount_written_differently_counts_as_the_same_payment(client):
    payment = fps_payment(amount="250.00")
    client.post("/payments", json=payment)

    response = client.post("/payments", json=payment | {"amount": "250"})

    assert response.status_code == 200


def test_reusing_uetr_for_a_different_payment_is_409(client):
    payment = fps_payment()
    client.post("/payments", json=payment)

    response = client.post("/payments", json=payment | {"amount": "999.00"})

    assert response.status_code == 409
    assert count_payments() == 1


def test_concurrent_duplicates_create_exactly_one_payment():
    # 10 copies of the same payment arrive at the same instant.
    request = PaymentRequest(**fps_payment())
    start_together = threading.Barrier(10)

    def submit(_):
        with SessionLocal() as session:
            start_together.wait()
            return submit_payment(session, request)[1]

    with ThreadPoolExecutor(max_workers=10) as pool:
        created_flags = list(pool.map(submit, range(10)))

    assert created_flags.count(True) == 1
    assert count_payments() == 1


@pytest.mark.parametrize(
    "overrides, problem",
    [
        ({"amount": "0"}, "zero amount"),
        ({"amount": "-10.00"}, "negative amount"),
        ({"amount": "10.001"}, "more than 2 decimal places"),
        ({"amount": "1000000.01"}, "over the FPS limit"),
        ({"currency": "EUR"}, "FPS must be GBP"),
        ({"creditor_iban": "DE89370400440532013000"}, "FPS creditor must be UK"),
        ({"creditor_iban": "GB82WEST12345698765433"}, "bad IBAN checksum"),
        ({"rail": "SWIFT"}, "unknown rail"),
        ({"uetr": "not-a-uuid"}, "bad UETR"),
        ({"debtor_name": ""}, "empty name"),
        ({"remittance_info": "x" * 141}, "reference over 140 chars"),
        ({"fee": "1.00"}, "unexpected field"),
    ],
)
def test_invalid_fps_payment_is_rejected(client, overrides, problem):
    response = client.post("/payments", json=fps_payment(**overrides))

    assert response.status_code == 422, problem
    assert count_payments() == 0


def test_sepa_must_be_in_euros(client):
    response = client.post("/payments", json=sepa_payment() | {"currency": "GBP"})

    assert response.status_code == 422
