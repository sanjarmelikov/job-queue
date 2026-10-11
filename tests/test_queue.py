import threading
import time

import psycopg
import pytest

from jobqueue.examples import flaky, noop, record_effect
from jobqueue.queue import Queue
from jobqueue.worker import Worker, backoff_seconds


def effects(dsn):
    with psycopg.connect(dsn) as conn:
        return dict(conn.execute("SELECT idempotency_key, deliveries FROM effects").fetchall())


def test_enqueue_claim_ack(dsn):
    q = Queue(dsn)
    job_id = q.enqueue({"value": 7}, idempotency_key="a")
    w = Worker(dsn, record_effect, poll_interval=0.05)
    assert w.run(max_jobs=1) == 1
    assert q.stats() == {"queued": 0, "running": 0, "done": 1, "dead": 0}
    assert effects(dsn) == {"a": 1}
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT status, attempts FROM jobs WHERE id = %s", (job_id,)).fetchone() == ("done", 1)
    w.close()


def test_idempotent_enqueue(dsn):
    q = Queue(dsn)
    first = q.enqueue({"v": 1}, idempotency_key="order-42")
    assert first is not None
    assert q.enqueue({"v": 1}, idempotency_key="order-42") is None  # same key: no second job
    assert q.enqueue_many([{"v": 2}, {"v": 3}], idempotency_keys=["order-42", "order-43"]) != []
    assert q.stats()["queued"] == 2


def test_two_workers_never_claim_the_same_job(dsn):
    q = Queue(dsn)
    q.enqueue_many([{"value": i} for i in range(200)], idempotency_keys=[f"k{i}" for i in range(200)])
    a, b = Worker(dsn, noop, batch=10), Worker(dsn, noop, batch=10)
    claimed = []
    for _ in range(10):
        claimed += [j.id for j in a.claim()] + [j.id for j in b.claim()]
    assert len(claimed) == 200 and len(set(claimed)) == 200  # each job exactly once
    a.close()
    b.close()


def test_retry_with_backoff_then_success(dsn):
    q = Queue(dsn)
    q.enqueue({"succeed_on": 3}, idempotency_key="f")
    w = Worker(dsn, flaky, poll_interval=0.05)
    w.run(max_jobs=1, idle_exit=10)  # fails twice (backoff 0–0.5s, 0–1s), succeeds on attempt 3
    with psycopg.connect(dsn) as conn:
        status, attempts, err = conn.execute("SELECT status, attempts, last_error FROM jobs").fetchone()
    assert (status, attempts) == ("done", 3) and "attempt 2 failed" in err
    assert effects(dsn) == {"f": 1}
    w.close()


def test_dead_letter_after_max_attempts(dsn):
    q = Queue(dsn)
    q.enqueue({"succeed_on": 99}, idempotency_key="doomed", max_attempts=2)
    w = Worker(dsn, flaky, poll_interval=0.05)
    w.run(idle_exit=3)
    assert q.stats() == {"queued": 0, "running": 0, "done": 0, "dead": 1}
    [dead] = q.dead_letters()
    assert dead["attempts"] == 2 and "attempt 2 failed" in dead["last_error"]
    assert effects(dsn) == {}  # failed attempts rolled back their side effects
    w.close()


def test_expired_lease_is_reclaimed_and_stale_ack_is_refused(dsn):
    q = Queue(dsn)
    q.enqueue({"value": 1}, idempotency_key="lease")
    crashed = Worker(dsn, record_effect, visibility_timeout=0.3, name="crashed")
    [job] = crashed.claim()  # takes the job... then "dies" without acking
    time.sleep(0.4)  # lease expires
    rescuer = Worker(dsn, record_effect, visibility_timeout=30, name="rescuer")
    [again] = rescuer.claim()
    assert again.id == job.id and again.attempts == 2  # second delivery
    rescuer.process(again)
    # The original worker wakes up and tries to finish its stale attempt: refused.
    crashed.process(job)
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT status, attempts FROM jobs").fetchone() == ("done", 2)
    assert effects(dsn) == {"lease": 2}  # ran twice, one effect row (idempotent), deliveries counted
    crashed.close()
    rescuer.close()


def test_notify_wakes_an_idle_worker_quickly(dsn):
    q = Queue(dsn)
    w = Worker(dsn, record_effect, poll_interval=10)  # would sleep 10 s without NOTIFY
    t = threading.Thread(target=w.run, kwargs={"max_jobs": 1})
    t.start()
    time.sleep(0.3)  # worker is now waiting
    start = time.perf_counter()
    q.enqueue({"value": 1}, idempotency_key="wake")
    t.join(timeout=5)
    assert not t.is_alive() and time.perf_counter() - start < 2
    w.close()


def test_backoff_grows_and_is_capped():
    import random

    random.seed(0)
    assert all(0 <= backoff_seconds(1) <= 0.5 for _ in range(100))
    assert max(backoff_seconds(5) for _ in range(1000)) > 4  # up to 8 s at attempt 5
    assert all(backoff_seconds(30) <= 60 for _ in range(100))


def test_graceful_stop_finishes_current_job(dsn):
    q = Queue(dsn)
    q.enqueue_many([{"value": i, "sleep": 0.2} for i in range(5)], idempotency_keys=[f"g{i}" for i in range(5)])
    w = Worker(dsn, record_effect, poll_interval=0.05)
    t = threading.Thread(target=w.run)
    t.start()
    time.sleep(0.3)
    w.stop()
    t.join(timeout=5)
    stats = q.stats()
    assert stats["running"] == 0  # the in-flight job was finished, not abandoned
    assert stats["done"] == w.processed >= 1
    w.close()


@pytest.mark.parametrize("bad", [{"value": "x"}])
def test_handler_error_is_recorded(dsn, bad):
    q = Queue(dsn)
    q.enqueue(bad, idempotency_key="bad", max_attempts=1)
    w = Worker(dsn, record_effect, poll_interval=0.05)
    w.run(idle_exit=1)
    [dead] = q.dead_letters()
    assert "invalid input syntax" in dead["last_error"] or "InvalidTextRepresentation" in dead["last_error"]
    w.close()


@pytest.mark.parametrize("ack_batch", [True, False])
def test_batch_and_single_acks_finish_every_job_once(dsn, ack_batch):
    q = Queue(dsn)
    q.enqueue_many([{"value": i} for i in range(55)], idempotency_keys=[f"b{i}" for i in range(55)])
    w = Worker(dsn, record_effect, batch=10, ack_batch=ack_batch, poll_interval=0.05)
    assert w.run(idle_exit=0.5) == 55
    assert q.stats()["done"] == 55 and set(effects(dsn).values()) == {1}
    w.close()


def test_batch_ack_skips_failed_jobs(dsn):
    q = Queue(dsn)
    q.enqueue_many([{"succeed_on": 1}, {"succeed_on": 99}, {"succeed_on": 1}],
                   idempotency_keys=["ok1", "bad", "ok2"], max_attempts=1)
    w = Worker(dsn, flaky, batch=10, poll_interval=0.05)
    w.run(idle_exit=0.5)
    assert q.stats() == {"queued": 0, "running": 0, "done": 2, "dead": 1}
    w.close()
