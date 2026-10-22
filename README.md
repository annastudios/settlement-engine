# settlement-engine

A payment processing engine in the style of a bank's back office: payment
instructions are accepted, queued, sent to (simulated) payment rails such as
Faster Payments and SEPA, retried safely when a rail fails, rate-limited so the
rails are never overloaded, and reconciled at the end of the day to prove that
no payment was lost or processed twice.

> Work in progress. Payments are submitted, queued, and sent by workers to
> a simulated rail that settles, rejects, fails and loses answers. Failed
> sends are retried with backoff, payments that keep failing land in a
> repair queue, payments left behind by crashed workers (or lost by Redis)
> are recovered, each rail has a rate limit shared by all workers, and
> every change is recorded in an append-only audit trail, an end-of-day
> reconciliation compares our ledger with the network's records, and
> Prometheus + Grafana graph it all.

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

Then open the **live dashboard** at http://localhost:8000/dashboard.

## Live dashboard

`/dashboard` refreshes every second and shows:

- How many payments are in each state
- Each rail's actual speed next to its rate limit, with buttons to change
  the limit
- What Redis holds right now: waiting in the queue, being worked on (which
  worker holds which payment, and how long its lease has left), waiting to
  retry
- The repair queue, with a **Resubmit** button per payment
- Recent payments, each row flashing when its status changes; click one to
  see its full history

Buttons send batches of random FPS/SEPA payments (about 1 in 10 FPS ones to
a closed account) and switch the rail simulator between **Healthy**,
**Flaky** (30% of sends fail), **Down** and **Loses answers** (settles, then
the answer is lost), so retries, the repair queue and recovery can be
watched as they happen. Kill a worker with Ctrl+C mid-batch and start it
again to watch crash recovery.

The `/demo/...` endpoints behind those buttons are for local development
and demos only.

Tests use a separate `settlement_test` database (created automatically), so
they never touch your dev data. Tables are created on startup; after a schema
change, reset the dev database with `docker compose down -v`.

## API

| Endpoint | What it does |
|---|---|
| `POST /payments` | Submit a payment. **201** new, **200** if this exact payment was already submitted, **409** if its UETR belongs to a different payment, **422** if invalid. |
| `GET /payments/{uetr}` | Look up a payment, including `attempts` and `last_error`. |
| `GET /payments/{uetr}/history` | The payment's audit trail: every change, who made it and why. |
| `GET /repair-queue` | Payments that ran out of attempts, oldest first. |
| `GET /reconciliation?day=YYYY-MM-DD` | End-of-day reconciliation against the network's records (default today, UTC). **503** if the network can't be reached. |
| `POST /payments/{uetr}/resubmit` | Give a payment in the repair queue a fresh set of attempts. **409** if it isn't in the repair queue. |
| `GET /health` | Postgres and Redis status; **503** if either is down. |
| `GET /dashboard` | The live dashboard page. |
| `GET /stats` | Everything the dashboard shows, as JSON. |
| `POST /demo/payments?count=N` | Submit N random test payments (1-200). Dev/demo only. |
| `PUT /demo/chaos` | Change the rail simulator's behaviour. Dev/demo only. |
| `PUT /demo/rate-limits` | Change a rail's rate limit for all workers, e.g. `{"rail": "FPS", "per_second": 5}`. Dev/demo only. |

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

## Rate limits

Each rail accepts at most N payments per second from us (`RATE_LIMIT_FPS`,
default 10; `RATE_LIMIT_SEPA`, default 5), however many workers are running.
The limit lives in Redis, so it is shared, and can be changed while running
(the dashboard has buttons for it).

Every payment **books the next free slot** on its rail (the GCRA algorithm,
a close cousin of the token bucket), in one atomic Lua step:

- Slot is now → sent straight away. Up to one second's worth go through as
  a burst.
- Slot is in the future → the payment is parked in the delayed set until
  then, and sent without booking again when it comes back. That is not a
  failed attempt and its status doesn't change.

Because workers park over-limit payments instead of waiting for a slot, a
busy rail never holds up the other one
(`test_busy_rail_does_not_hold_up_the_other_rail`), and parked payments
come back exactly at the rail's pace instead of all retrying at once.
`test_concurrent_workers_never_book_the_same_slot` has 8 workers book 80
slots at once: exactly 10 go now and the other 70 get their own slot, 100 ms
apart. Live, with 3 workers and FPS limited to 5/s, a batch of 100 finished
at 5 FPS payments per second after a one-second burst.

Changing a limit applies to new bookings; slots already booked keep their
times.

## Audit trail

Every change to a payment is recorded in `payment_events`: when, from which
status to which, the attempt number, **who** made it (`api`, `worker <id>`,
`recovery by worker <id>`, `ops`) and **why** (`rail answered 503; retrying
in 1.3s`, `rejected by the rail: AC04`, `recovered: its worker stopped while
holding it`, ...). See it with `GET /payments/{uetr}/history`, or click a
payment on the dashboard.

- **Written with the change, in the same transaction.** All status changes
  go through `audit.move()`, which locks the payment row, checks its current
  status, updates it and inserts the audit row, then commits. The change and
  its history entry are saved together or not at all, so the trail can never
  miss a change or describe one that didn't happen
  (`test_after_a_messy_run_every_payment_matches_its_trail`).
- **Append-only, enforced by Postgres.** A trigger rejects every `UPDATE` and
  `DELETE` on `payment_events`, so history can't be rewritten even by code
  or a person that bypasses this application.
- Timestamps use `clock_timestamp()`, so entries written in one transaction
  still get distinct, ordered times.

Payments created before the audit trail existed have no history.

## End-of-day reconciliation

Tests prove the code behaves; reconciliation checks what actually happened.
It compares our ledger with the payment network's own records (its
`GET /ledger`, standing in for the statement file a real network sends),
payment by payment, for one UTC day:

| Group | Meaning |
|---|---|
| **matched** | Same final status and amount on both sides |
| **pending** | Not finished here and not seen by the network yet (e.g. in the repair queue). Not a problem. |
| **break** | We and the network disagree. Needs a person. |

The breaks it detects: the network settled a payment we don't have as
settled (the money moved but we think it didn't), the network rejected one
we have as settled, a final payment the network has no record of, a payment
the network processed that we have no record of, and amount or currency
mismatches. It also compares settled counts and totals per currency, and
counts the resends the network's dedupe absorbed.

Run it from the dashboard ("Reconcile today"), with `GET /reconciliation`
(`?day=YYYY-MM-DD`), or as a command:

```bash
python -m settlement.reconciliation --day 2026-10-22
```

Live, a payment whose answers were all lost ended up in the repair queue
although the network had settled it. Reconciliation flagged it ("the
network settled it, but our record says otherwise"; GBP totals £560.00
apart). Resubmitting it with the same UETR made the network confirm instead
of paying again, and the next run balanced.

The simulator keeps its ledger in memory; set `RAIL_LEDGER_FILE` (e.g.
`rail_ledger.jsonl`) so it survives restarts, like a real network's would.

## Metrics (Prometheus + Grafana)

The API serves Prometheus metrics at `/metrics`. Most come straight from
the audit trail: it is append-only, so counting "entered settled", "entered
retrying" and so on gives counters that only ever grow, which is what
Prometheus needs, without adding anything to the workers.

| Metric | What it is |
|---|---|
| `settlement_payments_entered_total{status, rail}` | Payments entering each status. `received` = submitted, `processing` = sent to the rail, `retrying` = failed send. |
| `settlement_recoveries_total` | Times crash recovery acted on a payment |
| `settlement_payments{status}` | Payments in each status right now |
| `settlement_queue_payments{state}` | Redis queue right now: ready, in progress, waiting to retry |
| `settlement_rate_limit_per_second{rail}` | Each rail's limit in force |

Prometheus and Grafana run in Docker only when asked (about 75 MB of RAM
together when idle), with a ready-made dashboard:

```bash
docker compose --profile metrics up -d   # then open http://127.0.0.1:3000
```

The dashboard shows settled, rejected, in-repair and queued totals, settled
per second per rail against its rate limit, sends vs failed sends vs
rejections vs payments giving up, the Redis queue, and payments in flight.
An outage is visible at a glance: settled per second drops to zero, retries
climb, and the in-repair tile turns purple.
