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
