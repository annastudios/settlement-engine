# settlement-engine

A payment processing engine in the style of a bank's back office: payment
instructions are accepted, queued, sent to (simulated) payment rails such as
Faster Payments and SEPA, retried safely when a rail fails, rate-limited so the
rails are never overloaded, and reconciled at the end of the day to prove that
no payment was lost or processed twice.

> Work in progress. Payments are submitted, queued, and sent by workers to
> a simulated rail that settles, rejects, fails and loses answers. Failed
> sends are retried with backoff, payments that keep failing land in a
> repair queue, and payments left behind by crashed workers (or lost by
> Redis) are recovered. The rate limiter comes next.

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
| `GET /payments/{uetr}` | Look up a payment, including `attempts` and `last_error`. |
| `GET /repair-queue` | Payments that ran out of attempts, oldest first. |
| `POST /payments/{uetr}/resubmit` | Give a payment in the repair queue a fresh set of attempts. **409** if it isn't in the repair queue. |
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

```
received ──► processing ──► settled
                 │    ▲  └─► rejected            (final: e.g. AC04 closed account)
     unavailable │    │ due
                 ▼    │
              retrying ──► in_repair ──resubmit──► retrying
                   (after MAX_ATTEMPTS)
```

- **Atomic claim with a lease.** One Lua script pops the next payment *and*
  records a lease (deadline + owning worker) in a single step, so two workers
  can never claim the same payment. The deadline uses Redis's clock, not the
  worker's. Only the lease owner can release it.
- **Postgres guard.** A worker only starts a payment whose status is still
  `received` (a conditional `UPDATE`). Redis decides *who* works on a payment;
  Postgres guarantees it happens *once*, even if a UETR is queued twice.
- **Failures don't lose payments.** If processing fails unexpectedly (a bug,
  Postgres down), the lease is kept and the payment stays `processing` until
  crash recovery (below) picks it up. If Redis is down at submission, the
  payment is still accepted (it is safe in Postgres) and recovery queues it
  later.

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
  refused) → retried (below). It is resent **with the same UETR**, never as
  a new payment: if the first attempt actually settled, the rail's dedupe
  makes the resend harmless
  (`test_lost_answer_is_resolved_by_the_retry_without_paying_twice`).

## Retries and the repair queue

When the rail is unavailable the payment moves to `retrying` and waits
before its next attempt:

- **Exponential backoff:** about 1s, 2s, 4s, 8s ... (`RETRY_BASE_SECONDS`,
  capped at `RETRY_MAX_DELAY_SECONDS`), so a struggling rail gets room to
  recover.
- **Jitter:** each wait is a random 50-100% of that, so workers that failed
  together (the rail went down for everyone) don't all retry at the same
  moment.
- The wait lives in a Redis sorted set (`payments:delayed`, scored by due
  time from Redis's clock). The claim script first moves due retries back to
  the queue, then claims, in one atomic step, so no extra process is needed.

After `MAX_ATTEMPTS` sends (default 5) the payment moves to `in_repair`, the
repair queue (dead-letter queue), with `last_error` saying why. Ops list it
with `GET /repair-queue` and, once the problem is fixed, give it a fresh set
of attempts with `POST /payments/{uetr}/resubmit`. That is safe even if an
earlier attempt did go through: the same UETR is resent and the rail
dedupes it.

## Crash recovery

Every worker runs a recovery pass every `RECOVERY_INTERVAL_SECONDS` (10s),
so there is no extra process to forget to start. It handles:

| What happened | How it's spotted | What recovery does |
|---|---|---|
| A worker died (or stalled) holding a payment | Its lease passed `LEASE_SECONDS` (30s) | `processing` → `retrying` and re-queued; resent with the same UETR, so a send that secretly went through isn't paid twice |
| A worker died after finishing, before releasing its lease | Lease expired, payment already final | Lease cleaned up; **not** resent |
| A payment never reached Redis, or Redis was wiped | In flight in Postgres, unchanged for `ORPHAN_AFTER_SECONDS` (60s), unknown to Redis | Re-queued from Postgres |

- A crashed attempt still counts as an attempt, so a "poison" payment that
  crashes the worker every time ends in the repair queue instead of looping
  forever.
- Running it on many workers at once is safe: taking expired leases is one
  atomic Lua step (each goes to exactly one worker), and a payment queued
  twice is skipped by the Postgres guard.
- A payment changed recently (`updated_at`) is never treated as lost, so one
  a worker is handling right now is left alone.

This is what makes "Redis only coordinates" true:
`test_whole_queue_is_rebuilt_from_postgres_after_redis_is_wiped` empties
Redis completely and every payment still settles, each sent once.
