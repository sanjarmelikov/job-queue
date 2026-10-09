"""Load test: throughput and enqueue-to-start latency with N concurrent workers.

    python bench/load_test.py --dsn postgresql://... --workers 8 --rate 1000 --seconds 20
    python bench/load_test.py --dsn ... --workers 8 --burst 20000            # drain throughput
    python bench/load_test.py --dsn ... --workers 8 --burst 20000 --no-skip-locked --batch 1

Two modes:
  --rate R:   enqueue R jobs/second (in small batches) for --seconds; measures how long
              each job waited between enqueue and a worker starting it (p50/p95/p99).
  --burst N:  enqueue N jobs at once, then time how fast the workers drain them.
Latencies come from the database's own timestamps (enqueued_at, first_started_at),
so they include everything: commit, NOTIFY/poll wake-up, and the claim query.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from pathlib import Path

import psycopg

from jobqueue.queue import Queue

ROOT = Path(__file__).resolve().parents[1]


def start_workers(dsn: str, n: int, batch: int, skip_locked: bool) -> list[subprocess.Popen]:
    cmd = [sys.executable, "-m", "jobqueue", "worker", "--dsn", dsn, "--handler", "jobqueue.examples:noop",
           "--batch", str(batch), "--poll-interval", "0.1"]
    if not skip_locked:
        cmd.append("--no-skip-locked")
    return [subprocess.Popen(cmd, cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL) for _ in range(n)]


def wait_for_drain(q: Queue, total: int, timeout: float) -> float:
    start = time.monotonic()
    while time.monotonic() - start < timeout:
        if q.stats()["done"] >= total:
            return time.monotonic() - start
        time.sleep(0.02)
    raise TimeoutError(f"only {q.stats()['done']} of {total} jobs done after {timeout}s")


def latency_stats(dsn: str) -> dict:
    with psycopg.connect(dsn) as conn:
        row = conn.execute(
            """
            SELECT percentile_cont(ARRAY[0.5, 0.95, 0.99]) WITHIN GROUP (
                       ORDER BY extract(epoch FROM first_started_at - enqueued_at) * 1000),
                   count(*)
            FROM jobs WHERE first_started_at IS NOT NULL
            """
        ).fetchone()
    p50, p95, p99 = row[0]
    return {"wait_p50_ms": round(p50, 2), "wait_p95_ms": round(p95, 2), "wait_p99_ms": round(p99, 2)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--batch", type=int, default=10, help="jobs claimed per query")
    parser.add_argument("--no-skip-locked", action="store_true")
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--rate", type=float, help="steady enqueue rate, jobs/second")
    mode.add_argument("--burst", type=int, help="enqueue this many at once, then drain")
    parser.add_argument("--seconds", type=float, default=20)
    parser.add_argument("--label", default="")
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "load_test.jsonl")
    args = parser.parse_args()

    with psycopg.connect(args.dsn, autocommit=True) as conn:
        conn.execute("TRUNCATE jobs, dead_letters")
        conn.execute("VACUUM ANALYZE jobs")
    q = Queue(args.dsn)
    workers = start_workers(args.dsn, args.workers, args.batch, not args.no_skip_locked)
    time.sleep(1.0)  # let workers connect and start listening
    try:
        if args.burst:
            for start in range(0, args.burst, 1000):
                q.enqueue_many([{"i": i} for i in range(start, min(start + 1000, args.burst))])
            began = time.monotonic()
            total = args.burst
            elapsed = wait_for_drain(q, total, timeout=600)
            enqueue_rate = None
        else:
            chunk = max(1, int(args.rate / 50))  # enqueue in 50 small batches per second
            total, began = 0, time.monotonic()
            while time.monotonic() - began < args.seconds:
                q.enqueue_many([{"i": total + i} for i in range(chunk)])
                total += chunk
                next_at = began + total / args.rate
                time.sleep(max(0.0, next_at - time.monotonic()))
            enqueue_rate = total / (time.monotonic() - began)
            elapsed = (time.monotonic() - began) + wait_for_drain(q, total, timeout=600)
    finally:
        for w in workers:
            w.terminate()
        for w in workers:
            w.wait()

    result = {
        "mode": "burst" if args.burst else "rate",
        "workers": args.workers,
        "batch": args.batch,
        "skip_locked": not args.no_skip_locked,
        "jobs": total,
        "target_rate": args.rate,
        "achieved_enqueue_rate": round(enqueue_rate, 1) if enqueue_rate else None,
        "throughput_jobs_per_s": round(total / elapsed, 1),
        **latency_stats(args.dsn),
        "label": args.label,
        "commit": subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT,
                                 capture_output=True, text=True).stdout.strip(),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
    }
    q.close()
    print(json.dumps(result, indent=2))
    args.out.parent.mkdir(exist_ok=True)
    with open(args.out, "a") as f:
        f.write(json.dumps(result) + "\n")


if __name__ == "__main__":
    main()
