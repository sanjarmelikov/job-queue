"""Producer side: create the schema, enqueue jobs, inspect the queue."""

from __future__ import annotations

import json
import re
from datetime import datetime
from importlib import resources
from typing import Any

import psycopg
from psycopg.types.json import Jsonb


def connect(dsn: str, **kwargs) -> psycopg.Connection:
    return psycopg.connect(dsn, autocommit=True, **kwargs)


def init_db(dsn: str) -> None:
    sql = resources.files("jobqueue").joinpath("schema.sql").read_text()
    with connect(dsn) as conn:
        conn.execute(sql)


# Queue names end up in a LISTEN channel name, so they're restricted to a safe
# alphabet; anything else is rejected rather than escaped.
_QUEUE_NAME = re.compile(r"^[A-Za-z0-9_]{1,50}$")


def validate_queue(name: str) -> str:
    if not isinstance(name, str) or not _QUEUE_NAME.match(name):
        raise ValueError(f"invalid queue name {name!r}: use 1-50 letters, digits or underscores")
    return name


def notify_channel(queue: str) -> str:
    return f"jobs_{validate_queue(queue)}"


class Queue:
    def __init__(self, dsn: str):
        self.conn = connect(dsn)

    def close(self) -> None:
        self.conn.close()

    def enqueue(
        self,
        payload: dict[str, Any],
        queue: str = "default",
        idempotency_key: str | None = None,
        run_at: datetime | None = None,
        max_attempts: int = 5,
    ) -> int | None:
        """Add a job. Returns its id, or None if a job with this idempotency key already exists."""
        ids = self.enqueue_many([payload], queue, [idempotency_key], run_at, max_attempts)
        return ids[0] if ids else None

    def enqueue_many(
        self,
        payloads: list[dict[str, Any]],
        queue: str = "default",
        idempotency_keys: list[str | None] | None = None,
        run_at: datetime | None = None,
        max_attempts: int = 5,
    ) -> list[int]:
        """Add many jobs in one round trip. Keys that already exist are skipped (not errors)."""
        channel = notify_channel(queue)  # validates the queue name
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        keys = idempotency_keys or [None] * len(payloads)
        for p in payloads:  # serialize now, so a bad payload fails before any SQL runs
            json.dumps(p)
        wrapped = [Jsonb(p) for p in payloads]
        with self.conn.transaction():
            rows = self.conn.execute(
                """
                INSERT INTO jobs (queue, payload, idempotency_key, run_at, max_attempts)
                SELECT %s, p, k, coalesce(%s, now()), %s
                FROM unnest(%s::jsonb[], %s::text[]) AS t(p, k)
                ON CONFLICT (idempotency_key) DO NOTHING
                RETURNING id
                """,
                (queue, run_at, max_attempts, wrapped, keys),
            ).fetchall()
            # Wake sleeping workers right away instead of waiting for their next poll.
            # pg_notify takes the channel as a parameter: no string-built SQL.
            self.conn.execute("SELECT pg_notify(%s, '')", (channel,))
        return [r[0] for r in rows]

    def stats(self, queue: str = "default") -> dict[str, int]:
        counts = dict(
            self.conn.execute(
                "SELECT status, count(*) FROM jobs WHERE queue = %s GROUP BY status", (queue,)
            ).fetchall()
        )
        dead = self.conn.execute("SELECT count(*) FROM dead_letters WHERE queue = %s", (queue,)).fetchone()[0]
        return {s: counts.get(s, 0) for s in ("queued", "running", "done")} | {"dead": dead}

    def dead_letters(self, queue: str = "default") -> list[dict]:
        cur = self.conn.execute(
            "SELECT id, payload, attempts, last_error FROM dead_letters WHERE queue = %s ORDER BY id", (queue,)
        )
        return [dict(zip(("id", "payload", "attempts", "last_error"), row)) for row in cur.fetchall()]

