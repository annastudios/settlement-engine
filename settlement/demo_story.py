"""The one-click demo: a scripted story of about 90 seconds, played on the
live system while the dashboard narrates it.

  1. A normal day                payments flow in and settle
  2. The network gets flaky      30% of sends fail; they retry and settle
  3. A worker crashes            a worker is killed mid-send; another finishes
  4. The network goes down       nothing gets through; payments go to repair
  5. Ops fixes it                network back; the repair queue is resubmitted
  6. End of day                  reconciliation against the network's records

It starts its own two workers (real processes), so act 3 can kill one for
real. Whatever happens, it stops its workers and puts the network back to
healthy at the end. For local development and demos only.
"""

import logging
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy import func, select

from settlement import audit, connections, rail_client, reconciliation, work_queue
from settlement.demo_data import random_payment
from settlement.models import Payment
from settlement.payments import submit_payment
from settlement.schemas import PaymentStatus

log = logging.getLogger("settlement.demo")

ACTS = [
    ("A normal day", "Payments flow in and settle."),
    ("The network gets flaky", "30% of sends fail. Watch them retry and still settle."),
    ("A worker crashes mid-payment", "A worker is killed while sending. Another one finishes its work."),
    ("The network goes down", "Nothing gets through, so payments end up in the repair queue."),
    ("Ops fixes it", "The network is back and the repair queue is resubmitted."),
    ("End of day", "Reconciliation: does everything match the network's records?"),
]
HEALTHY = {"latency_ms": 150}
_PROJECT = Path(__file__).resolve().parents[1]
_FINAL = (PaymentStatus.SETTLED, PaymentStatus.REJECTED)


class StopRequested(Exception):
    pass


class WorkerProcess:
    """A worker started by the demo, as its own process, so it can be killed."""

    def __init__(self, name: str):
        self.name = name
        # Named by us, not by process id: on Windows the venv's python.exe is
        # a launcher, so the worker inside has a different id than Popen sees.
        self.worker_id = f"{socket.gethostname()}-{name.replace(' ', '-')}-{uuid.uuid4().hex[:6]}"
        # Short lease and frequent recovery, so a crash is recovered within
        # seconds instead of the usual half a minute.
        self.process = subprocess.Popen(
            [sys.executable, "-m", "settlement.worker"],
            cwd=_PROJECT,
            env=os.environ | {
                "WORKER_ID": self.worker_id,
                "LEASE_SECONDS": "6",
                "RECOVERY_INTERVAL_SECONDS": "2",
            },
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    def kill(self) -> None:
        """A hard crash: no chance to finish the payment or clean up."""
        if sys.platform == "win32":
            # Kill the launcher and the real worker inside it.
            subprocess.run(
                ["taskkill", "/PID", str(self.process.pid), "/T", "/F"],
                capture_output=True, check=False,
            )
        else:
            self.process.kill()
        self.process.wait(timeout=10)

    def stop(self) -> None:
        if self.process.poll() is None:
            self.kill()


class DemoStory:
    def __init__(
        self,
        start_worker: Callable[[str], WorkerProcess] = WorkerProcess,
        speed: float = 1.0,
    ):
        self._start_worker = start_worker
        self._speed = speed  # below 1 makes every pause shorter (tests)
        self._lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._state = self._fresh_state()

    @staticmethod
    def _fresh_state() -> dict:
        return {"running": False, "act": 0, "log": [], "result": None, "error": None}

    # --- control ----------------------------------------------------------------

    def start(self) -> bool:
        """Start the story in the background. False if it is already running."""
        with self._lock:
            if self._state["running"]:
                return False
            self._state = self._fresh_state() | {"running": True}
            self._stop.clear()
            self._thread = threading.Thread(target=self.run, name="demo-story", daemon=True)
            self._thread.start()
            return True

    def stop(self) -> None:
        self._stop.set()

    def status(self) -> dict:
        with self._lock:
            state = dict(self._state, log=list(self._state["log"]))
        act = state["act"]
        state["acts"] = [
            {
                "number": n,
                "title": title,
                "description": description,
                "state": "done" if n < act or (n == act and not state["running"])
                else "current" if n == act else "next",
            }
            for n, (title, description) in enumerate(ACTS, start=1)
        ]
        return state

    # --- the story ----------------------------------------------------------------

    def run(self) -> None:
        workers = []
        try:
            self._begin_act(1)
            self._chaos(HEALTHY)
            workers = [self._start_worker("demo worker A"), self._start_worker("demo worker B")]
            self._say("Started two demo workers, A and B.")
            self._send(40)
            self._pause(12)

            self._begin_act(2)
            self._chaos({"latency_ms": 150, "failure_rate": 0.3})
            self._send(30)
            self._pause(15)

            self._begin_act(3)
            self._crash_a_worker(workers)

            self._begin_act(4)
            self._chaos({"failure_rate": 1})
            down = self._send(5, closed_account_chance=0)
            self._wait_for(lambda: self._all_in(down, (PaymentStatus.IN_REPAIR,)), 90)
            self._say("All 5 ran out of attempts and are in the repair queue, waiting for a person.")

            self._begin_act(5)
            self._chaos(HEALTHY)
            self._say("The network is healthy again. Ops resubmits the 5 from the repair queue.")
            self._resubmit(down)
            self._wait_for(lambda: self._all_in(down, _FINAL), 60)
            self._say("All 5 settled. Same IDs, so nothing could be paid twice.")

            self._begin_act(6)
            self._wait_for(self._nothing_in_flight, 120)
            self._end_of_day()
        except StopRequested:
            self._say("Demo stopped.")
        except Exception as exc:  # report it on the dashboard rather than die silently
            log.exception("demo story failed")
            with self._lock:
                self._state["error"] = str(exc)
            self._say(f"Something went wrong: {exc}")
        finally:
            for worker in workers:
                try:
                    worker.stop()
                except Exception:
                    log.exception("could not stop %s", worker.name)
            try:
                rail_client.set_chaos(HEALTHY)
            except Exception:
                log.exception("could not put the network back to healthy")
            with self._lock:
                self._state["running"] = False

    def _crash_a_worker(self, workers: list) -> None:
        victim, survivor = workers
        # Slow network, so the worker is caught in the middle of a send.
        self._chaos({"latency_ms": 1500})
        self._send(10, closed_account_chance=0)
        held = self._wait_for(lambda: self._held_by(victim.worker_id), 30)
        payment = self._payment(held)
        victim.kill()
        self._say(
            f"Killed {victim.name} while it was sending {_money(payment)} "
            f"from {payment.debtor_name} to {payment.creditor_name}."
        )
        self._wait_for(lambda: self._payment(held).status in _FINAL, 60)
        payment = self._payment(held)
        self._say(
            f"Its claim ran out and the payment was picked up again: {payment.status} "
            f"on attempt {payment.attempts}. {survivor.name} carries on."
        )
        self._chaos(HEALTHY)
        self._pause(4)

    def _end_of_day(self) -> None:
        with connections.SessionLocal() as session:
            report = reconciliation.run_for_day(session, datetime.now(UTC).date())
        verdict = (
            f"Balanced: all {report['matched']} of today's payments match the network's records."
            if report["balanced"]
            else f"{len(report['breaks'])} break(s) need a person."
        )
        self._say(f"Reconciliation: {verdict} The network absorbed {report['resends_absorbed']} resends.")
        with self._lock:
            self._state["result"] = {
                "balanced": report["balanced"],
                "matched": report["matched"],
                "breaks": len(report["breaks"]),
                "resends_absorbed": report["resends_absorbed"],
            }

    # --- helpers --------------------------------------------------------------------

    def _begin_act(self, number: int) -> None:
        self._check_stop()
        with self._lock:
            self._state["act"] = number
        title, description = ACTS[number - 1]
        self._say(f"Act {number}: {title}. {description}")

    def _say(self, text: str) -> None:
        log.info("demo: %s", text)
        with self._lock:
            self._state["log"].append({"at": datetime.now(UTC).isoformat(), "text": text})

    def _check_stop(self) -> None:
        if self._stop.is_set():
            raise StopRequested

    def _pause(self, seconds: float) -> None:
        if self._stop.wait(seconds * self._speed):
            raise StopRequested

    def _wait_for(self, check: Callable, timeout: float):
        """Poll check() until it returns something truthy; return that.
        Real seconds (not scaled by speed): it waits for the system, not
        for the audience."""
        deadline = time.monotonic() + timeout
        while True:
            result = check()
            if result:
                return result
            if time.monotonic() >= deadline:
                raise TimeoutError(f"gave up waiting after {timeout:.0f}s")
            if self._stop.wait(0.3):
                raise StopRequested

    def _chaos(self, settings: dict) -> None:
        rail_client.set_chaos(settings)

    def _send(self, count: int, closed_account_chance: float = 0.1) -> list[uuid.UUID]:
        uetrs = []
        with connections.SessionLocal() as session:
            for _ in range(count):
                payment, _ = submit_payment(
                    session, random_payment(closed_account_chance), actor="demo story"
                )
                work_queue.enqueue(connections.redis_client, str(payment.uetr))
                uetrs.append(payment.uetr)
        self._say(f"Sent {count} payments.")
        return uetrs

    def _resubmit(self, uetrs: list[uuid.UUID]) -> None:
        with connections.SessionLocal() as session:
            for uetr in uetrs:
                if audit.move(
                    session, uetr, (PaymentStatus.IN_REPAIR,), PaymentStatus.RETRYING,
                    actor="ops (demo story)", reason="resubmitted from the repair queue",
                    attempts=0,
                ):
                    work_queue.enqueue(connections.redis_client, str(uetr))

    def _held_by(self, worker_id: str) -> uuid.UUID | None:
        for uetr, owner in connections.redis_client.hgetall(work_queue.LEASE_OWNERS).items():
            if owner == worker_id:
                return uuid.UUID(uetr)
        return None

    def _payment(self, uetr: uuid.UUID) -> Payment:
        with connections.SessionLocal() as session:
            return session.get(Payment, uetr)

    def _all_in(self, uetrs: list[uuid.UUID], statuses: tuple) -> bool:
        with connections.SessionLocal() as session:
            return session.scalar(
                select(func.count()).where(Payment.uetr.in_(uetrs), Payment.status.in_(statuses))
            ) == len(uetrs)

    def _nothing_in_flight(self) -> bool:
        with connections.SessionLocal() as session:
            return not session.scalar(
                select(func.count()).where(Payment.status.in_(
                    (PaymentStatus.RECEIVED, PaymentStatus.PROCESSING, PaymentStatus.RETRYING)
                ))
            )


def _money(payment: Payment) -> str:
    symbol = {"GBP": "£", "EUR": "€"}.get(payment.currency, f"{payment.currency} ")
    return f"{symbol}{payment.amount:,.2f}"
