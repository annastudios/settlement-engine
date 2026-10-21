# settlement-engine

A payment processing engine in the style of a bank's back office: payment
instructions are accepted, queued, sent to (simulated) payment rails such as
Faster Payments and SEPA, retried safely when a rail fails, rate-limited so the
rails are never overloaded, and reconciled at the end of the day to prove that
no payment was lost or processed twice.

> Work in progress. This is step 0: the project skeleton.

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
uvicorn settlement.main:app --reload # then open http://localhost:8000/health
```
