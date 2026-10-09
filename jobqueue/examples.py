"""Example handlers, also used by the tests and benchmarks.

`record_effect` is the idempotent pattern: the job's side effect (here, a row
in `effects`) is written in the handler's transaction, keyed by the job's
idempotency key. If the job runs twice (at-least-once delivery), the second
insert hits the unique key and does nothing, so the effect happens exactly once.
"""

from __future__ import annotations

import time

import psycopg

from jobqueue.worker import Job

EFFECTS_SQL = """
CREATE TABLE IF NOT EXISTS effects (
    idempotency_key TEXT PRIMARY KEY,
    job_id          BIGINT NOT NULL,
    value           BIGINT NOT NULL,
    deliveries      INT    NOT NULL DEFAULT 1   -- how many times the job actually ran
)
"""


def record_effect(job: Job, conn: psycopg.Connection) -> None:
    key = job.idempotency_key or f"job-{job.id}"
    if job.payload.get("sleep"):
        time.sleep(job.payload["sleep"])  # simulate work (gives fault injection a window)
    conn.execute(
        """
        INSERT INTO effects (idempotency_key, job_id, value) VALUES (%s, %s, %s)
        ON CONFLICT (idempotency_key) DO UPDATE SET deliveries = effects.deliveries + 1
        """,
        (key, job.id, job.payload.get("value", 0)),
    )


def flaky(job: Job, conn: psycopg.Connection) -> None:
    """Fails until its `succeed_on` attempt (for retry and dead-letter tests)."""
    if job.attempts < job.payload.get("succeed_on", 10**9):
        raise RuntimeError(f"attempt {job.attempts} failed")
    record_effect(job, conn)


def noop(job: Job, conn: psycopg.Connection) -> None:
    """Does nothing: measures pure queue overhead in benchmarks."""
