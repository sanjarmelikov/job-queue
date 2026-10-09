"""Fault injection: kill -9 workers mid-job and prove no job is lost.

    python bench/fault_injection.py --dsn postgresql://... --jobs 10000 --workers 8

Starts N worker processes, enqueues jobs whose handler writes an idempotent side
effect, then repeatedly SIGKILLs a random worker (no cleanup, no ack: exactly a
crash) and starts a replacement. When the queue drains it checks:
  - lost:        jobs whose effect never happened (must be 0)
  - duplicates:  extra deliveries caused by the crashes (expected > 0, and harmless,
                 because the effect table's idempotency key absorbed them)
"""

from __future__ import annotations

import argparse
import json
import random
import subprocess
import sys
import time
from pathlib import Path

import psycopg

from jobqueue.queue import Queue

ROOT = Path(__file__).resolve().parents[1]


def start_worker(dsn: str, visibility: float) -> subprocess.Popen:
    return subprocess.Popen(
        [sys.executable, "-m", "jobqueue", "worker", "--dsn", dsn, "--batch", "5",
         "--visibility-timeout", str(visibility), "--poll-interval", "0.2"],
        cwd=ROOT, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )


def run(dsn: str, jobs: int, workers: int, kill_every: float, visibility: float, timeout: float) -> dict:
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("TRUNCATE jobs, dead_letters, effects")
    q = Queue(dsn)
    for start in range(0, jobs, 1000):  # enqueue in chunks of 1000
        n = min(1000, jobs - start)
        q.enqueue_many(
            [{"value": i, "sleep": random.uniform(0, 0.004)} for i in range(start, start + n)],
            idempotency_keys=[f"job-{i}" for i in range(start, start + n)],
        )

    procs = [start_worker(dsn, visibility) for _ in range(workers)]
    kills = 0
    began = time.monotonic()
    next_kill = began + kill_every
    try:
        while time.monotonic() - began < timeout:
            stats = q.stats()
            if stats["done"] + stats["dead"] == jobs:
                break
            if time.monotonic() >= next_kill:
                victim = random.randrange(len(procs))
                procs[victim].kill()  # SIGKILL: the worker gets no chance to ack or roll back
                procs[victim].wait()
                procs[victim] = start_worker(dsn, visibility)
                kills += 1
                next_kill = time.monotonic() + kill_every
            time.sleep(0.05)
        elapsed = time.monotonic() - began
    finally:
        for p in procs:
            p.terminate()
        for p in procs:
            p.wait()

    with psycopg.connect(dsn) as conn:
        effect_keys = {r[0] for r in conn.execute("SELECT idempotency_key FROM effects")}
        extra_deliveries = conn.execute("SELECT coalesce(sum(deliveries - 1), 0) FROM effects").fetchone()[0]
        redelivered_jobs = conn.execute("SELECT count(*) FROM jobs WHERE attempts > 1").fetchone()[0]
    expected = {f"job-{i}" for i in range(jobs)}
    stats = q.stats()
    q.close()
    return {
        "jobs": jobs,
        "workers": workers,
        "kills": kills,
        "visibility_timeout_s": visibility,
        "seconds": round(elapsed, 1),
        "done": stats["done"],
        "dead": stats["dead"],
        "lost": len(expected - effect_keys),
        "redelivered_jobs": redelivered_jobs,
        "duplicate_deliveries_absorbed": int(extra_deliveries),
        "unexpected_effects": len(effect_keys - expected),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dsn", required=True)
    parser.add_argument("--jobs", type=int, default=10_000)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--kill-every", type=float, default=0.5, help="seconds between SIGKILLs")
    parser.add_argument("--visibility", type=float, default=2.0, help="lease length, seconds")
    parser.add_argument("--timeout", type=float, default=600)
    parser.add_argument("--out", type=Path, default=ROOT / "results" / "fault_injection.jsonl")
    args = parser.parse_args()
    result = run(args.dsn, args.jobs, args.workers, args.kill_every, args.visibility, args.timeout)
    result["commit"] = subprocess.run(["git", "rev-parse", "--short", "HEAD"], cwd=ROOT,
                                      capture_output=True, text=True).stdout.strip()
    result["timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S")
    print(json.dumps(result, indent=2))
    args.out.parent.mkdir(exist_ok=True)
    with open(args.out, "a") as f:
        f.write(json.dumps(result) + "\n")


if __name__ == "__main__":
    main()
