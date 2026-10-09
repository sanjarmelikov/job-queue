"""Consumer side: claim jobs, run handlers, acknowledge or retry.

Delivery is at-least-once. A job is leased to one worker for `visibility_timeout`
seconds; if the worker dies before acknowledging, the lease runs out and another
worker runs the job again. So handlers must be idempotent: running a job twice
must have the same effect as running it once. Idempotency keys (see
`examples.py`) are the standard way to get there.
"""

from __future__ import annotations

import logging
import os
import random
import select
import signal
import socket
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import psycopg

from jobqueue.queue import connect, notify_channel

log = logging.getLogger(__name__)

# Claim up to `batch` runnable jobs and lease them to this worker, in one statement.
# FOR UPDATE SKIP LOCKED: rows another worker is claiming right now are skipped
# instead of waited on, so concurrent workers never block each other and never
# take the same job.
CLAIM_SQL = """
WITH next AS (
    SELECT id FROM jobs
    WHERE queue = %(queue)s AND status <> 'done' AND run_at <= now()
    ORDER BY run_at
    LIMIT %(batch)s
    FOR UPDATE SKIP LOCKED
)
UPDATE jobs SET
    status = 'running',
    attempts = jobs.attempts + 1,
    run_at = now() + make_interval(secs => %(visibility)s),
    locked_by = %(worker)s,
    first_started_at = coalesce(jobs.first_started_at, now())
FROM next
WHERE jobs.id = next.id
RETURNING jobs.id, jobs.payload, jobs.attempts, jobs.max_attempts, jobs.idempotency_key
"""

# Variant without SKIP LOCKED, kept for the contention benchmark: workers queue
# up behind each other's row locks on the same head-of-queue rows.
CLAIM_SQL_BLOCKING = CLAIM_SQL.replace("FOR UPDATE SKIP LOCKED", "FOR UPDATE")

# Both guarded by (locked_by, attempts): a worker whose lease expired and whose job
# was re-claimed by someone else can't mark that newer attempt done or failed.
ACK_SQL = """
UPDATE jobs SET status = 'done', finished_at = now(), locked_by = NULL
WHERE id = %(id)s AND locked_by = %(worker)s AND attempts = %(attempts)s AND status = 'running'
"""
RETRY_SQL = """
UPDATE jobs SET status = 'queued', run_at = now() + make_interval(secs => %(delay)s),
    locked_by = NULL, last_error = %(error)s
WHERE id = %(id)s AND locked_by = %(worker)s AND attempts = %(attempts)s AND status = 'running'
"""
DEAD_SQL = """
WITH gone AS (
    DELETE FROM jobs
    WHERE id = %(id)s AND locked_by = %(worker)s AND attempts = %(attempts)s AND status = 'running'
    RETURNING id, queue, payload, attempts, idempotency_key, enqueued_at
)
INSERT INTO dead_letters (id, queue, payload, attempts, idempotency_key, last_error, enqueued_at)
SELECT id, queue, payload, attempts, idempotency_key, %(error)s, enqueued_at FROM gone
"""


@dataclass
class Job:
    id: int
    payload: dict[str, Any]
    attempts: int
    max_attempts: int
    idempotency_key: str | None


Handler = Callable[[Job, psycopg.Connection], None]


def backoff_seconds(attempt: int, base: float = 0.5, cap: float = 60.0) -> float:
    """Exponential backoff with full jitter: random in [0, min(cap, base * 2^(attempt-1))].

    The randomness spreads retries out, so a burst of failures doesn't come back as a
    synchronized burst of retries (the "thundering herd").
    """
    return random.uniform(0, min(cap, base * 2 ** (attempt - 1)))


class Worker:
    def __init__(
        self,
        dsn: str,
        handler: Handler,
        queue: str = "default",
        batch: int = 1,
        visibility_timeout: float = 30.0,
        poll_interval: float = 1.0,
        skip_locked: bool = True,
        name: str | None = None,
    ):
        self.dsn = dsn
        self.handler = handler
        self.queue = queue
        self.batch = batch
        self.visibility_timeout = visibility_timeout
        self.poll_interval = poll_interval
        self.claim_sql = CLAIM_SQL if skip_locked else CLAIM_SQL_BLOCKING
        self.name = name or f"{socket.gethostname()}:{os.getpid()}"
        self.conn = connect(dsn)  # claims, acks, retries
        self.handler_conn = psycopg.connect(dsn)  # handlers run their own transactions here
        self.listen_conn = connect(dsn)  # LISTEN for "new job" notifications
        self.listen_conn.execute(f"LISTEN {notify_channel(queue)}")
        self._stopping = False
        self.processed = 0

    def stop(self, *_: Any) -> None:
        """Finish the current batch, then exit (graceful shutdown on SIGTERM/SIGINT)."""
        self._stopping = True

    def claim(self) -> list[Job]:
        rows = self.conn.execute(
            self.claim_sql,
            {"queue": self.queue, "batch": self.batch, "visibility": self.visibility_timeout, "worker": self.name},
        ).fetchall()
        return [Job(*row) for row in rows]

    def process(self, job: Job) -> None:
        params = {"id": job.id, "worker": self.name, "attempts": job.attempts}
        try:
            self.handler(job, self.handler_conn)
            self.handler_conn.commit()
        except Exception as exc:  # noqa: BLE001 - any handler failure means retry or dead-letter
            self.handler_conn.rollback()
            error = f"{type(exc).__name__}: {exc}"[:2000]
            if job.attempts >= job.max_attempts:
                self.conn.execute(DEAD_SQL, params | {"error": error})
                log.warning("job %d dead after %d attempts: %s", job.id, job.attempts, error)
            else:
                self.conn.execute(RETRY_SQL, params | {"delay": backoff_seconds(job.attempts), "error": error})
            return
        if self.conn.execute(ACK_SQL, params).rowcount == 0:
            # Our lease expired and someone else took the job: it will run (again) there.
            log.warning("job %d: lease lost before ack; another worker owns it now", job.id)
        self.processed += 1

    def wait_for_work(self) -> None:
        """Sleep until a NOTIFY arrives or poll_interval passes (delayed retries need the poll)."""
        select.select([self.listen_conn.fileno()], [], [], self.poll_interval)
        for _ in self.listen_conn.notifies(timeout=0):
            pass  # drain; we only care that something arrived

    def run(self, max_jobs: int | None = None, idle_exit: float | None = None) -> int:
        """Process jobs until stopped (or `max_jobs` done, or idle for `idle_exit` seconds)."""
        idle_since = time.monotonic()
        while not self._stopping and (max_jobs is None or self.processed < max_jobs):
            jobs = self.claim()
            if not jobs:
                if idle_exit is not None and time.monotonic() - idle_since > idle_exit:
                    break
                self.wait_for_work()
                continue
            idle_since = time.monotonic()
            for job in jobs:
                self.process(job)
        return self.processed

    def close(self) -> None:
        for c in (self.conn, self.handler_conn, self.listen_conn):
            c.close()


def run_worker(dsn: str, handler: Handler, **kwargs: Any) -> None:
    """Entry point for a worker process, with graceful shutdown on SIGTERM/SIGINT."""
    worker = Worker(dsn, handler, **kwargs)
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    try:
        worker.run()
    finally:
        worker.close()
