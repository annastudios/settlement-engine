"""A simulated payment rail (Faster Payments / SEPA), standing in for the
outside world our bank sends payments to.

Run it with:
    uvicorn rail_sim.main:app --port 9000

Like a real rail it settles payments, rejects payments to closed accounts,
and dedupes on the UETR: the same payment sent twice moves money once and
gets the original answer back. Chaos settings make it misbehave on purpose
(GET/PUT /chaos, or RAIL_* environment variables at startup).

The ledger is kept in memory. Set RAIL_LEDGER_FILE to also save each new
payment to that file (one JSON line each) and load it again at startup, so
the ledger survives restarts like a real network's would.
"""

import json
import os
import random
import threading
import time
from datetime import UTC, datetime
from decimal import Decimal

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

app = FastAPI(title="rail simulator")


class Instruction(BaseModel):
    uetr: str
    rail: str
    amount: Decimal
    currency: str
    debtor_iban: str
    creditor_iban: str


class Chaos(BaseModel):
    latency_ms: int = Field(default=0, ge=0)
    # Answer 503 before doing anything: nothing happened, safe to resend.
    failure_rate: float = Field(default=0, ge=0, le=1)
    # Settle the payment, then lose the answer (wait hang_seconds, then 504).
    # The sender cannot know the money moved: the case retries must survive.
    drop_response_rate: float = Field(default=0, ge=0, le=1)
    hang_seconds: float = Field(default=5, ge=0)


def _chaos_from_env() -> Chaos:
    return Chaos(
        latency_ms=int(os.getenv("RAIL_LATENCY_MS", "0")),
        failure_rate=float(os.getenv("RAIL_FAILURE_RATE", "0")),
        drop_response_rate=float(os.getenv("RAIL_DROP_RESPONSE_RATE", "0")),
        hang_seconds=float(os.getenv("RAIL_HANG_SECONDS", "5")),
    )


# Payments to these accounts are rejected with ISO 20022 reason code AC04
# (ClosedAccountNumber). Valid IBANs, so they pass our own validation.
CLOSED_ACCOUNTS = {"GB56CLOS00000000000001", "GB29CLOS00000000000002"}

chaos = _chaos_from_env()
# UETR -> what the rail did with it. The lock makes "seen it before?" and
# "record it" one step, so concurrent duplicates still settle once.
_ledger: dict[str, dict] = {}
_lock = threading.Lock()
_LEDGER_FILE = os.getenv("RAIL_LEDGER_FILE")


def _load_ledger() -> None:
    if _LEDGER_FILE and os.path.exists(_LEDGER_FILE):
        with open(_LEDGER_FILE, encoding="utf-8") as file:
            for line in file:
                entry = json.loads(line)
                _ledger[entry["uetr"]] = entry


def _save_entry(entry: dict) -> None:
    """Append a newly processed payment to the ledger file, if there is one.
    Only the first arrival is saved: later resends of the same payment
    (received_count) are counted in memory only."""
    if _LEDGER_FILE:
        with open(_LEDGER_FILE, "a", encoding="utf-8") as file:
            file.write(json.dumps(entry) + "\n")


_load_ledger()


def reset(new_chaos: Chaos | None = None) -> None:
    """Empty the ledger and set the chaos settings (used by tests)."""
    global chaos
    with _lock:
        _ledger.clear()
    chaos = new_chaos or Chaos()


@app.post("/payments")
def receive_payment(instruction: Instruction):
    if chaos.latency_ms:
        time.sleep(chaos.latency_ms / 1000)
    if random.random() < chaos.failure_rate:
        raise HTTPException(status_code=503, detail="rail temporarily unavailable")

    with _lock:
        entry = _ledger.get(instruction.uetr)
        if entry is None:
            entry = _decide(instruction)
            _ledger[instruction.uetr] = entry
            entry["received_count"] += 1
            _save_entry(entry)
        else:
            entry["received_count"] += 1
        answer = {"uetr": instruction.uetr, "status": entry["status"], "reason": entry["reason"]}

    if random.random() < chaos.drop_response_rate:
        time.sleep(chaos.hang_seconds)
        raise HTTPException(status_code=504, detail="response lost")
    return answer


def _decide(instruction: Instruction) -> dict:
    closed = instruction.creditor_iban in CLOSED_ACCOUNTS
    return {
        "uetr": instruction.uetr,
        "rail": instruction.rail,
        "amount": str(instruction.amount),
        "currency": instruction.currency,
        "status": "rejected" if closed else "settled",
        "reason": "AC04" if closed else None,
        "processed_at": datetime.now(UTC).isoformat(),
        # How many times this UETR arrived. Above 1 means a resend that the
        # dedupe absorbed; the money still moved once.
        "received_count": 0,
    }


@app.get("/payments/{uetr}")
def get_payment(uetr: str):
    entry = _ledger.get(uetr)
    if entry is None:
        raise HTTPException(status_code=404, detail="rail has no record of this payment")
    return entry


@app.get("/ledger")
def ledger():
    """Everything the rail has processed: its side of end-of-day
    reconciliation (a real network sends this as a statement file)."""
    with _lock:
        return [dict(entry) for entry in _ledger.values()]


@app.get("/chaos")
def get_chaos() -> Chaos:
    return chaos


@app.put("/chaos")
def set_chaos(new_chaos: Chaos) -> Chaos:
    global chaos
    chaos = new_chaos
    return chaos
