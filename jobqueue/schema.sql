-- Durable job queue schema (PostgreSQL 13+).
--
-- A job's life: queued → running (leased to one worker until run_at) → done,
-- or back to queued with a backoff delay if it fails, or into dead_letters
-- after too many attempts. A running job whose lease expires (the worker died)
-- is claimable again: that's the visibility timeout, and why delivery is
-- at-least-once rather than exactly-once.

CREATE TABLE IF NOT EXISTS jobs (
    id               BIGSERIAL PRIMARY KEY,
    queue            TEXT        NOT NULL DEFAULT 'default',
    payload          JSONB       NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'queued'
                     CHECK (status IN ('queued', 'running', 'done')),
    attempts         INT         NOT NULL DEFAULT 0,
    max_attempts     INT         NOT NULL DEFAULT 5,
    -- queued: earliest time to run (now, or later after a backoff)
    -- running: when the lease expires and another worker may take over
    run_at           TIMESTAMPTZ NOT NULL DEFAULT now(),
    locked_by        TEXT,
    -- Enqueue-side dedupe: the same key enqueued twice creates one job.
    idempotency_key  TEXT UNIQUE,
    last_error       TEXT,
    enqueued_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    first_started_at TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ
);

-- The claim query only ever looks at unfinished jobs, ordered by run_at. A
-- partial index keeps it small and fast no matter how many done jobs pile up.
CREATE INDEX IF NOT EXISTS jobs_claimable ON jobs (queue, run_at) WHERE status <> 'done';

CREATE TABLE IF NOT EXISTS dead_letters (
    id               BIGINT PRIMARY KEY,  -- same id the job had
    queue            TEXT        NOT NULL,
    payload          JSONB       NOT NULL,
    attempts         INT         NOT NULL,
    idempotency_key  TEXT,
    last_error       TEXT,
    enqueued_at      TIMESTAMPTZ NOT NULL,
    died_at          TIMESTAMPTZ NOT NULL DEFAULT now()
);
