"""End-of-day reconciliation: compare our ledger with the payment network's
records, payment by payment, and report every disagreement.

Tests prove the code behaves; reconciliation checks what actually happened.
Every payment of the day ends up in one of three groups:

  matched  we and the network agree (same final status, same amount)
  pending  not finished on our side and the network hasn't seen it yet
  break    we and the network disagree: a person has to look at it

Run it from the dashboard, with GET /reconciliation, or as a command:
    python -m settlement.reconciliation [--day YYYY-MM-DD]
"""

import argparse
from collections import Counter
from datetime import UTC, date, datetime, time, timedelta
from decimal import Decimal

from sqlalchemy import select
from sqlalchemy.orm import Session

from settlement import connections, rail_client
from settlement.models import Payment
from settlement.schemas import PaymentStatus

_FINAL = (PaymentStatus.SETTLED, PaymentStatus.REJECTED)

# What each kind of break means, in plain words, for whoever reads the report.
BREAKS = {
    "network_settled_we_did_not": (
        "The network settled it (the money moved), but our record says otherwise."
    ),
    "network_rejected_we_did_not": "The network rejected it, but our record says otherwise.",
    "missing_at_network": "We have a final answer for it, but the network has no record of it.",
    "unknown_to_us": "The network processed a payment we have no record of.",
    "amount_mismatch": "We and the network disagree on the amount or currency.",
}


def reconcile(ours: list[dict], network: list[dict]) -> dict:
    """Compare two sets of records, matched by UETR.

    ours:    {"uetr", "status", "amount", "currency"} per payment
    network: {"uetr", "status", "amount", "currency", "received_count"} per payment
    """
    ours_by_uetr = {p["uetr"]: p for p in ours}
    network_by_uetr = {n["uetr"]: n for n in network}
    matched = 0
    pending = Counter()
    breaks = []

    for uetr in sorted(ours_by_uetr.keys() | network_by_uetr.keys()):
        our = ours_by_uetr.get(uetr)
        theirs = network_by_uetr.get(uetr)
        if our is None:
            breaks.append(_break(uetr, "unknown_to_us", our, theirs))
        elif theirs is None:
            if our["status"] in _FINAL:
                breaks.append(_break(uetr, "missing_at_network", our, theirs))
            else:
                pending[our["status"]] += 1
        elif (Decimal(our["amount"]), our["currency"]) != (
            Decimal(theirs["amount"]), theirs["currency"]
        ):
            breaks.append(_break(uetr, "amount_mismatch", our, theirs))
        elif our["status"] == theirs["status"]:
            matched += 1
        elif theirs["status"] == PaymentStatus.SETTLED:
            breaks.append(_break(uetr, "network_settled_we_did_not", our, theirs))
        else:
            breaks.append(_break(uetr, "network_rejected_we_did_not", our, theirs))

    return {
        "balanced": not breaks,
        "matched": matched,
        "pending": dict(pending),
        "breaks": breaks,
        "settled_totals": {"ours": _settled_totals(ours), "network": _settled_totals(network)},
        # Copies of a payment the network received more than once (resends
        # after lost answers or crashes) that its dedupe absorbed.
        "resends_absorbed": sum(max(0, n.get("received_count", 1) - 1) for n in network),
    }


def _break(uetr: str, kind: str, our: dict | None, theirs: dict | None) -> dict:
    return {
        "uetr": uetr,
        "kind": kind,
        "explanation": BREAKS[kind],
        "our_status": our["status"] if our else None,
        "network_status": theirs["status"] if theirs else None,
        "our_amount": f"{our['amount']} {our['currency']}" if our else None,
        "network_amount": f"{theirs['amount']} {theirs['currency']}" if theirs else None,
    }


def _settled_totals(records: list[dict]) -> dict:
    """How many payments settled, and how much money, per currency."""
    totals: dict[str, dict] = {}
    for record in records:
        if record["status"] != PaymentStatus.SETTLED:
            continue
        currency = totals.setdefault(record["currency"], {"count": 0, "amount": Decimal("0")})
        currency["count"] += 1
        currency["amount"] += Decimal(record["amount"])
    return {code: {"count": t["count"], "amount": str(t["amount"])} for code, t in sorted(totals.items())}


def run_for_day(session: Session, day: date) -> dict:
    """Reconcile one business day (UTC): our payments created that day, plus
    anything the network processed that day."""
    start = datetime.combine(day, time.min, UTC)
    end = start + timedelta(days=1)

    ours = {
        str(p.uetr): _as_record(p)
        for p in session.scalars(
            select(Payment).where(Payment.created_at >= start, Payment.created_at < end)
        )
    }
    network = [
        n for n in rail_client.fetch_ledger()
        if n["uetr"] in ours or start <= datetime.fromisoformat(n["processed_at"]) < end
    ]
    # Something the network processed today may have been submitted on an
    # earlier day: bring in our record of it rather than call it unknown.
    earlier = [n["uetr"] for n in network if n["uetr"] not in ours]
    if earlier:
        for p in session.scalars(select(Payment).where(Payment.uetr.in_(earlier))):
            ours[str(p.uetr)] = _as_record(p)

    return {"day": day.isoformat(), **reconcile(list(ours.values()), network)}


def _as_record(payment: Payment) -> dict:
    return {
        "uetr": str(payment.uetr),
        "status": payment.status,
        "amount": str(payment.amount),
        "currency": payment.currency,
    }


def _print_report(report: dict) -> None:
    print(f"Reconciliation for {report['day']} (UTC)")
    print("=" * 44)
    print("BALANCED: we and the network agree" if report["balanced"]
          else f"NOT BALANCED: {len(report['breaks'])} break(s) need a person")
    print(f"\nMatched:  {report['matched']}")
    pending = ", ".join(f"{n} {status}" for status, n in report["pending"].items()) or "none"
    print(f"Pending:  {pending}")
    print(f"Resends the network absorbed: {report['resends_absorbed']}")
    print("\nSettled totals          ours                network")
    currencies = report["settled_totals"]["ours"].keys() | report["settled_totals"]["network"].keys()
    for code in sorted(currencies):
        ours = report["settled_totals"]["ours"].get(code, {"count": 0, "amount": "0"})
        theirs = report["settled_totals"]["network"].get(code, {"count": 0, "amount": "0"})
        print(f"  {code}  {ours['count']:>5} | {ours['amount']:>12}   {theirs['count']:>5} | {theirs['amount']:>12}")
    for b in report["breaks"]:
        print(f"\nBREAK {b['uetr']}: {b['explanation']}")
        print(f"  ours: {b['our_status']} {b['our_amount']}   network: {b['network_status']} {b['network_amount']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="End-of-day reconciliation")
    parser.add_argument("--day", type=date.fromisoformat, default=datetime.now(UTC).date(),
                        help="business day to reconcile, YYYY-MM-DD (UTC); default today")
    args = parser.parse_args()
    with connections.SessionLocal() as session:
        _print_report(run_for_day(session, args.day))
