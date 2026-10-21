import uuid
from concurrent.futures import ThreadPoolExecutor

CLOSED_ACCOUNT = "GB56CLOS00000000000001"


def instruction(**overrides):
    body = {
        "uetr": str(uuid.uuid4()),
        "rail": "FPS",
        "amount": "250.00",
        "currency": "GBP",
        "debtor_iban": "GB33BUKB20201555555555",
        "creditor_iban": "GB82WEST12345698765432",
    }
    return body | overrides


def test_settles_a_payment_and_records_it(rail):
    body = instruction()

    response = rail.post("/payments", json=body)

    assert response.status_code == 200
    assert response.json() == {"uetr": body["uetr"], "status": "settled", "reason": None}
    ledger = rail.get(f"/payments/{body['uetr']}").json()
    assert ledger["status"] == "settled"
    assert ledger["amount"] == "250.00"


def test_rejects_payment_to_closed_account_with_ac04(rail):
    response = rail.post("/payments", json=instruction(creditor_iban=CLOSED_ACCOUNT))

    assert response.json()["status"] == "rejected"
    assert response.json()["reason"] == "AC04"


def test_same_uetr_twice_gets_the_original_answer_and_moves_money_once(rail):
    body = instruction()

    first = rail.post("/payments", json=body).json()
    second = rail.post("/payments", json=body).json()

    assert second == first
    assert rail.get(f"/payments/{body['uetr']}").json()["received_count"] == 2


def test_concurrent_copies_are_recorded_once(rail):
    body = instruction()

    with ThreadPoolExecutor(max_workers=10) as pool:
        answers = list(pool.map(lambda _: rail.post("/payments", json=body).json(), range(10)))

    assert all(answer["status"] == "settled" for answer in answers)
    assert rail.get(f"/payments/{body['uetr']}").json()["received_count"] == 10


def test_failure_answers_503_and_records_nothing(rail):
    rail.put("/chaos", json={"failure_rate": 1})
    body = instruction()

    response = rail.post("/payments", json=body)

    assert response.status_code == 503
    assert rail.get(f"/payments/{body['uetr']}").status_code == 404


def test_dropped_response_still_moves_the_money(rail):
    # The scary case: the rail settles, but the sender never hears back.
    rail.put("/chaos", json={"drop_response_rate": 1, "hang_seconds": 0})
    body = instruction()

    response = rail.post("/payments", json=body)

    assert response.status_code == 504
    assert rail.get(f"/payments/{body['uetr']}").json()["status"] == "settled"


def test_unknown_payment_is_404(rail):
    assert rail.get(f"/payments/{uuid.uuid4()}").status_code == 404


def test_chaos_settings_can_be_read_back(rail):
    rail.put("/chaos", json={"latency_ms": 5, "failure_rate": 0.25})

    chaos = rail.get("/chaos").json()

    assert chaos["latency_ms"] == 5
    assert chaos["failure_rate"] == 0.25
