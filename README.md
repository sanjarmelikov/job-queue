# job-queue

A durable job queue in Python on PostgreSQL, with at-least-once delivery. Workers claim jobs with `FOR UPDATE SKIP LOCKED`; a claim is a lease (visibility timeout), so a crashed worker's job is re-delivered; failures retry with exponential backoff; jobs that keep failing go to a dead-letter table. Idempotency keys make duplicate deliveries harmless.

```python
from jobqueue import Queue, Worker
from jobqueue.examples import record_effect

Queue(dsn).enqueue({"value": 42}, idempotency_key="order-42")
Worker(dsn, record_effect, batch=10).run()
```

```bash
docker compose up --scale worker=8    # Postgres + schema + 8 workers
```

## Design

| Concern | How |
|---|---|
| **Claiming** | One statement claims up to `batch` runnable jobs: `SELECT … ORDER BY run_at LIMIT n FOR UPDATE SKIP LOCKED`, then `UPDATE … SET status='running', run_at = now() + visibility_timeout`. Rows another worker is claiming are *skipped*, not waited on. |
| **Leases** | A running job's `run_at` is its lease expiry. If the worker dies, the job becomes claimable again when the lease runs out. That's what makes delivery **at-least-once**. |
| **Fencing** | Ack, retry and dead-letter updates only apply `WHERE locked_by = me AND attempts = my attempt`, so a worker whose lease expired can't overwrite a newer attempt. |
| **Retries** | Exponential backoff with full jitter (random in `[0, min(60 s, 0.5 s × 2^(attempt-1))]`), so failures don't come back as a synchronized burst. |
| **Dead letters** | After `max_attempts`, the job moves (in one statement) to `dead_letters` with its last error. |
| **Idempotency** | Enqueue: a unique `idempotency_key` makes re-enqueueing a no-op. Processing: handlers record their effect under the same key (`examples.record_effect`), so a re-delivered job can't apply its effect twice. |
| **Wake-ups** | `NOTIFY` on enqueue wakes idle workers immediately (`LISTEN`), with polling as a fallback for delayed retries. |
| **Batched acks** | Successful jobs in a claimed batch are acknowledged with one `UPDATE … FROM unnest(ids, attempts)`. `--ack-each` acks one at a time. |
| **Shutdown** | SIGTERM/SIGINT finishes the current batch, then exits. |

The claim query is backed by a partial index `(queue, run_at) WHERE status <> 'done'`, so it stays small however many finished jobs accumulate.

## Testing

```bash
python3 -m venv .venv && .venv/bin/pip install -e '.[dev]'
.venv/bin/pytest          # starts a throwaway Postgres cluster (initdb + pg_ctl) and removes it afterwards
```

Tests run against real PostgreSQL 17: claim/ack, idempotent enqueue, two workers never claiming the same job, retry with backoff, dead-lettering, an expired lease being re-claimed while the stale worker's late ack is refused, NOTIFY wake-up latency, graceful stop, batched vs per-job acks, and fault injection (below).

## Fault injection: 0 of 10,000 jobs lost

`bench/fault_injection.py` runs 8 worker processes over 10,000 jobs while:
- **SIGKILLing a random worker every 0.5 s** (no cleanup, no ack) and starting a replacement, and
- **making workers crash right after committing a job's effect but before acking it** (`--crash-after-commit 0.005`). That's the worst moment, and random kills almost never land there.

| Run | Crashes | Lost | Re-delivered jobs | Duplicate effects absorbed by idempotency key |
|---|---:|---:|---:|---:|
| SIGKILL only (commit `cfed168`) | 11 | **0** | 24 | 0 |
| SIGKILL + crash-after-commit (commit `947b4f5`) | 13 + 53 | **0** | 206 | **54** |

Every job finished, and every effect row exists exactly once. Results: `results/fault_injection.jsonl`.

## Throughput

8 workers on an Apple M2 (8 GB) with Postgres on the same machine, no-op handler, draining a 20,000-job burst, claim batch 10. Throughput is measured from the database's own timestamps (first job start to last job finish). Medians of 3 alternating runs, commit `12527d4`, rows in `results/load_test.jsonl`.

| Variant | Runs (jobs/s) | Median |
|---|---|---:|
| Per-job acks | 17,453 · 17,127 · 16,914 | 17,127 |
| **Batched acks** | 24,048 · 24,594 · 24,466 | **24,466 (+43%)** |
| Batched acks, plain `FOR UPDATE` (no SKIP LOCKED) | 6,996 · 6,074 · 43,400 | 6,996 |
| **Batched acks, `FOR UPDATE SKIP LOCKED`** | 23,685 · 24,276 · 24,253 | **24,253 (3.5×)** |

- **Lock contention:** without `SKIP LOCKED`, workers queue behind each other's row locks on the same head-of-queue jobs. With it, they each take different rows. The plain `FOR UPDATE` variant is also unstable (one run at 43K/s, the others 6–7K/s), which is why medians are reported.
- **Batched acks:** with per-job acks, every job's acknowledgement was its own transaction and WAL flush. One `UPDATE` per batch removed most of those round trips. Tradeoff: if a worker dies mid-batch, the batch's finished-but-unacked jobs are re-delivered (absorbed by idempotency).

### Latency under steady load

Same setup, but a producer enqueues at a fixed rate for 15 s while the 8 workers run. "Wait" is enqueue-to-start time from the database's timestamps (`first_started_at - enqueued_at`), so it includes commit, NOTIFY wake-up and the claim query. Commit `12527d4`, run 2026-10-10 under `caffeinate` (an earlier attempt was discarded after `pmset -g log` showed the machine had slept mid-run).

| Enqueue rate | Throughput | Wait p50 | Wait p95 | Wait p99 |
|---:|---:|---:|---:|---:|
| 1,000 jobs/s | 1,002 | 1.1 ms | 2.1 ms | 6.2 ms |
| 2,000 jobs/s | 2,002 | 1.3 ms | 2.0 ms | 2.7 ms |
| 3,000 jobs/s | 3,004 | 1.6 ms | 3.0 ms | 8.6 ms |
| 4,000 jobs/s | 4,004 | 1.7 ms | 2.7 ms | 6.8 ms |
| 5,000 jobs/s | 5,005 | 1.6 ms | **4.0 ms** | 7.8 ms |
| 6,000 jobs/s | 6,010 | 3.1 ms | 25.7 ms | 51.3 ms |

Workers keep up at every rate. p95 stays under 4 ms up to 5,000 jobs/s, thanks to LISTEN/NOTIFY wake-ups; at 6,000 jobs/s the tail grows as the producer, Postgres and 8 workers compete for the same 8 cores (the burst drain above, with no producer competing, reaches about 24K jobs/s).

## Limitations

One queue table for all queues; no priorities or job dependencies; no scheduled/cron jobs beyond `run_at`; `done` rows accumulate (add a periodic delete or partitioning in production).
