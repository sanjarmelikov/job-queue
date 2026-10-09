"""Small version of bench/fault_injection.py, run with the test suite."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "bench"))

from fault_injection import run  # noqa: E402


def test_killing_workers_mid_job_loses_nothing(dsn):
    result = run(dsn, jobs=1000, workers=4, kill_every=0.3, visibility=1.0, timeout=120)
    assert result["kills"] >= 1
    assert result["done"] == 1000 and result["dead"] == 0
    assert result["lost"] == 0 and result["unexpected_effects"] == 0


def test_crash_between_commit_and_ack_duplicates_are_absorbed(dsn):
    # Workers die right after committing a job's effect but before acking it, so those
    # jobs are delivered again; the idempotency key turns the repeat into a no-op.
    result = run(dsn, jobs=500, workers=4, kill_every=60, visibility=0.5, timeout=120, crash_after_commit=0.02)
    assert result["crashes_after_commit"] >= 1 and result["duplicate_deliveries_absorbed"] >= 1
    assert result["done"] == 500 and result["lost"] == 0 and result["unexpected_effects"] == 0
