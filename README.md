# settlement-engine

A payment processing engine in the style of a bank's back office: payment
instructions are accepted, queued, sent to (simulated) payment rails such as
Faster Payments and SEPA, retried safely when a rail fails, rate-limited so the
rails are never overloaded, and reconciled at the end of the day to prove that
no payment was lost or processed twice.

> Work in progress. Payments can be submitted and looked up; processing
> comes next.

## Design in one line

**Postgres holds the money truth** (payments and an append-only audit trail).
**Redis only coordinates work** (queue, leases, retries, rate limits). If Redis
is wiped, nothing is lost: the queue can be rebuilt from Postgres.

## Run it locally

Requires Python 3.13 and Docker.

```bash
docker compose up -d                 # Postgres + Redis
python -m venv .venv
.venv\Scripts\activate               # Windows; on macOS/Linux: source .venv/bin/activate
pip install -r requirements-dev.txt
pytest -v
uvicorn settlement.main:app --reload # then open http://localhost:8000/docs
```

Tests use a separate `settlement_test` database (created automatically), so
they never touch your dev data. Tables are created on startup; after a schema
change, reset the dev database with `docker compose down -v`.

## API

| Endpoint | What it does |
|---|---|
| `POST /payments` | Submit a payment. **201** new, **200** if this exact payment was already submitted, **409** if its UETR belongs to a different payment, **422** if invalid. |
| `GET /payments/{uetr}` | Look up a payment. |
| `GET /health` | Postgres and Redis status; **503** if either is down. |

A payment is loosely modelled on ISO 20022 pacs.008:

```json
{
  "uetr": "7c1628ac-9580-47bf-be9a-d9dfce7a48d5",
  "rail": "FPS",
  "amount": "250.00",
  "currency": "GBP",
  "debtor_name": "Alice Smith",
  "debtor_iban": "GB33BUKB20201555555555",
  "creditor_name": "Bob's Bakery Ltd",
  "creditor_iban": "GB82WEST12345698765432",
  "remittance_info": "Invoice 1042"
}
```

Validation: IBANs pass the ISO 13616 mod-97 checksum; FPS is GBP only, up
to £1,000,000, to a UK IBAN; SEPA is EUR only; amounts are positive with at
most 2 decimal places and are stored as `NUMERIC`, never float; text fields
follow ISO 20022's 140-character limit.

**Idempotency:** the UETR is the primary key, and submissions use
`INSERT ... ON CONFLICT DO NOTHING`, so Postgres decides atomically. Even 10
copies of a payment arriving at the same instant store exactly one (see
`test_concurrent_duplicates_create_exactly_one_payment`).
