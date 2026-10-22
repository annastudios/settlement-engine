# Settlement Engine

[![Tests](https://github.com/annastudios/settlement-engine/actions/workflows/tests.yml/badge.svg)](https://github.com/annastudios/settlement-engine/actions/workflows/tests.yml)

A bank-style payment engine: it accepts payment instructions, sends them to
simulated payment networks (Faster Payments, SEPA), and keeps working when
networks fail, workers crash or replies get lost, without ever **losing** a
payment or **paying one twice**.

## What it guarantees

- **Never paid twice.** Every payment has a unique ID (UETR). Duplicates are
  stored once, and resends after a failure reuse the same ID, which the
  network deduplicates.
- **Never lost.** Postgres is the source of truth; Redis only coordinates work.
  Crashed workers and even a wiped Redis are recovered from Postgres.
- **Never stuck silently.** Payments that keep failing go to a repair queue
  with the reason, for a person to resubmit.
- **Provable.** An append-only audit trail and an end-of-day reconciliation
  against the network's own records.

## How it works

```
client ──► API (FastAPI) ──► Postgres (ledger + audit trail)
                │
                └──► Redis queue ──► workers ──► payment network (simulated)
```

- **Idempotency:** the UETR is the primary key; `INSERT … ON CONFLICT` lets
  Postgres decide atomically, so even simultaneous duplicates store one row.
- **Atomic claim:** a Redis Lua script takes a payment *and* records an owned,
  time-limited lease in one step, so two workers can't take the same one.
- **Exactly-once effect:** a worker only starts a payment whose status is still
  waiting (a compare-and-swap `UPDATE`), even if Redis hands it out twice.
- **Retries:** capped exponential backoff with jitter (≈1, 2, 4, 8 s…); after 5
  attempts the payment moves to the repair queue (dead-letter queue).
- **Crash recovery:** expired leases are reclaimed atomically; payments missing
  from Redis are re-queued from Postgres; a payment that crashes workers every
  time ends in repair instead of looping.
- **Rate limits:** each network has a per-second limit shared by all workers
  (GCRA slot booking in Redis); over-limit payments wait for their slot without
  blocking other networks.
- **Audit trail:** every status change and its history row are written in one
  transaction; a Postgres trigger refuses any edit or delete.
- **Reconciliation:** compares our ledger with the network's records, payment by
  payment, and flags every disagreement.
- **Metrics:** Prometheus counters (derived from the audit trail) and a
  ready-made Grafana dashboard.

## Try it

Requires Python 3.13 and Docker.

```bash
docker compose up -d                   # Postgres + Redis
python -m venv .venv
.venv\Scripts\activate                 # macOS/Linux: source .venv/bin/activate
pip install -r requirements-dev.txt
pytest                                 # 178 tests, against real Postgres and Redis
```

Then, each in its own terminal:

```bash
uvicorn rail_sim.main:app --port 9000  # simulated payment network
uvicorn settlement.main:app            # API: http://127.0.0.1:8000/docs
python -m settlement.worker            # a worker (run several for more)
```

Open the **live dashboard** at http://127.0.0.1:8000/dashboard. It shows a
live scoreboard (*lost · sent twice · stuck*, all checked against the
network's records), payments moving through their states, rate limits, the
repair queue, and each payment's full history in one click. Buttons make the
network healthy, flaky, down, or lose its replies.

For Grafana graphs: `docker compose --profile metrics up -d`, then
http://127.0.0.1:3000.

### The 90-second demo

**▶ Run the 90-second demo** on the dashboard plays a narrated story on the
live system: payments flow, the network gets flaky, a worker is **killed
mid-payment**, the network goes down and payments land in the repair queue,
ops resubmits them, and the day ends with reconciliation. From a real run:

```
Killed demo worker A while it was sending £4,590.20 from Alice Smith to City Council Tax.
Its claim ran out and the payment was picked up again: settled on attempt 2.
All 5 settled. Same IDs, so nothing could be paid twice.
Reconciliation: Balanced: all 601 of today's payments match the network's records.
```

## API

| Endpoint | |
|---|---|
| `POST /payments` | Submit a payment. **201** new, **200** repeat of the same payment, **409** UETR reused for a different one |
| `GET /payments/{uetr}` | A payment, with attempts and last error |
| `GET /payments/{uetr}/history` | Its audit trail: every change, who made it and why |
| `GET /repair-queue`, `POST /payments/{uetr}/resubmit` | Payments needing a person, and giving one a fresh start |
| `GET /reconciliation` | Today's reconciliation against the network's records |
| `GET /health`, `GET /metrics` | Health check, Prometheus metrics |

Full interactive docs at `/docs` when running. A payment looks like this
(loosely ISO 20022 pacs.008):

```json
{
  "uetr": "7c1628ac-9580-47bf-be9a-d9dfce7a48d5",
  "rail": "FPS",
  "amount": "250.00",
  "currency": "GBP",
  "debtor_name": "Alice Smith",
  "debtor_iban": "GB33BUKB20201555555555",
  "creditor_name": "Bob's Bakery Ltd",
  "creditor_iban": "GB82WEST12345698765432"
}
```

## Built with

Python 3.13 · FastAPI · PostgreSQL · SQLAlchemy · Redis (Lua scripts) ·
Docker Compose · Prometheus · Grafana · pytest · GitHub Actions

The payment networks are simulated; no real money moves.
