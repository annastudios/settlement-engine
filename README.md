# settlement-engine

A payment processing engine in the style of a bank's back office: payment
instructions are accepted, queued, sent to (simulated) payment rails such as
Faster Payments and SEPA, retried safely when a rail fails, rate-limited so the
rails are never overloaded, and reconciled at the end of the day to prove that
no payment was lost or processed twice.

> Work in progress. Payments are submitted, queued, and sent by workers to
> a simulated rail that settles, rejects, fails and loses answers. Retries
> come next.

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
```

Then, each in its own terminal (with the venv activated):

```bash
uvicorn rail_sim.main:app --port 9000  # the simulated payment rail
uvicorn settlement.main:app --reload   # the API: http://localhost:8000/docs
python -m settlement.worker            # a worker; run several for more
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

## Processing

A new payment is stored in Postgres, then its UETR is pushed onto a Redis
queue. Workers (`python -m settlement.worker`) take payments off the queue:

`received` → `processing` → `settled` or `rejected`

- **Atomic claim with a lease.** One Lua script pops the next payment *and*
  records a lease (deadline + owning worker) in a single step, so two workers
  can never claim the same payment. The deadline uses Redis's clock, not the
  worker's. Only the lease owner can release it.
- **Postgres guard.** A worker only starts a payment whose status is still
  `received` (a conditional `UPDATE`). Redis decides *who* works on a payment;
  Postgres guarantees it happens *once*, even if a UETR is queued twice.
- **Failures don't lose payments.** If processing fails, the lease is kept and
  the payment stays `processing`; recovering expired leases is a later step.
  If Redis is down at submission, the payment is still accepted (it is safe in
  Postgres) and waits in `received`.

Proven by `test_concurrent_workers_never_claim_the_same_payment` (8 workers
racing for 300 payments: none claimed twice, none missed).

## The rail simulator

`rail_sim/` stands in for Faster Payments / SEPA: a separate service, like the
real outside world. It settles payments, rejects payments to closed accounts
with ISO 20022 reason code `AC04`, and **dedupes on the UETR**: a payment
sent twice gets the original answer and moves money once.

Chaos settings (`PUT /chaos`, or `RAIL_*` environment variables) make it
misbehave on purpose:

| Setting | What happens | What it means for us |
|---|---|---|
| `failure_rate` | `503` before anything happens | Nothing moved; safe to resend |
| `drop_response_rate` | Settles, then hangs and answers `504` | Money moved but we never heard: **the outcome is unknown** |
| `latency_ms` | Slow answers | |

The worker's rail client turns every answer into one of three outcomes:

- **settled** → `settled`
- **rejected** → `rejected` with the reason code. Final: never resent.
- **unavailable** (5xx, timeout after `RAIL_TIMEOUT_SECONDS`, connection
  refused) → the payment stays `processing` with its lease kept. It must be
  resent **with the same UETR**, never as a new payment: if the first attempt
  actually settled, the rail's dedupe makes the resend harmless
  (`test_resending_after_a_lost_answer_moves_the_money_once`).
