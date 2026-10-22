"""Numbers for Prometheus, served at GET /metrics and read every few seconds.

Most of them come straight from the audit trail: every change a payment goes
through is already recorded there, and an append-only table only ever grows,
which is exactly what a Prometheus counter needs. Counting "entered settled",
"entered retrying" and so on gives settled/retries/failures per second
without adding anything to the workers. (At very high volume you'd count in
the workers instead of querying; here one grouped query per scrape is fine.)
"""

import redis
from prometheus_client import CollectorRegistry
from prometheus_client.core import CounterMetricFamily, GaugeMetricFamily
from sqlalchemy import func, or_, select

from settlement import connections, rate_limit, work_queue
from settlement.models import Payment, PaymentEvent
from settlement.schemas import PaymentStatus


class SettlementCollector:
    """Computes every metric when Prometheus asks, so the numbers are never stale."""

    def collect(self):
        with connections.SessionLocal() as session:
            transitions = session.execute(
                select(PaymentEvent.to_status, Payment.rail, func.count())
                .join(Payment, Payment.uetr == PaymentEvent.uetr)
                # Only real status changes, not notes like "put back in the queue".
                .where(or_(PaymentEvent.from_status.is_(None),
                           PaymentEvent.from_status != PaymentEvent.to_status))
                .group_by(PaymentEvent.to_status, Payment.rail)
            ).all()
            recoveries = session.scalar(
                select(func.count()).where(PaymentEvent.actor.like("recovery%"))
            )
            current = dict(
                session.execute(select(Payment.status, func.count()).group_by(Payment.status)).all()
            )

        entered = CounterMetricFamily(
            "settlement_payments_entered",
            "Payments entering each status, by rail (from the audit trail). "
            "received = submitted, processing = sent to the rail, retrying = a failed send.",
            labels=["status", "rail"],
        )
        for status, rail, count in transitions:
            entered.add_metric([status, rail], count)
        yield entered

        recovered = CounterMetricFamily(
            "settlement_recoveries",
            "Times crash recovery acted on a payment (worker stopped, or Redis lost it)",
        )
        recovered.add_metric([], recoveries)
        yield recovered

        by_status = GaugeMetricFamily(
            "settlement_payments", "Payments currently in each status", labels=["status"]
        )
        for status in PaymentStatus:
            by_status.add_metric([status.value], current.get(status.value, 0))
        yield by_status

        yield from _redis_gauges(connections.redis_client)


def _redis_gauges(r: redis.Redis):
    try:
        queue = {
            "ready": r.llen(work_queue.READY),
            "in_progress": r.zcard(work_queue.LEASES),
            "waiting_to_retry": r.zcard(work_queue.DELAYED),
        }
        limits = rate_limit.limits(r)
    except redis.RedisError:
        return  # Redis down: leave these out rather than report zeros
    queued = GaugeMetricFamily(
        "settlement_queue_payments", "Payments in the Redis queue right now", labels=["state"]
    )
    for state, count in queue.items():
        queued.add_metric([state], count)
    yield queued

    limit = GaugeMetricFamily(
        "settlement_rate_limit_per_second", "Each rail's rate limit in force", labels=["rail"]
    )
    for rail, per_second in limits.items():
        limit.add_metric([rail], per_second)
    yield limit


registry = CollectorRegistry()
registry.register(SettlementCollector())
