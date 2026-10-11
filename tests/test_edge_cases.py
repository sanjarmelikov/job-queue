"""Edge cases and hostile inputs for the queue."""

import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
import pytest

from jobqueue.examples import flaky, noop, record_effect
from jobqueue.queue import Queue
from jobqueue.worker import Worker


def effects(dsn):
    with psycopg.connect(dsn) as conn:
        return dict(conn.execute("SELECT idempotency_key, deliveries FROM effects").fetchall())


# ---- payloads ----

def test_odd_payloads_round_trip(dsn):
    q = Queue(dsn)
    payloads = [{}, {"emoji": "🚀", "ru": "привет"}, {"nested": {"a": [1, None, {"b": True}]}},
                {"big": "x" * 1_000_000}]
    q.enqueue_many(payloads)
    w = Worker(dsn, noop, batch=10)
    jobs = w.claim()
    assert sorted(len(str(j.payload)) for j in jobs) == sorted(len(str(p)) for p in payloads)
    w.close()


def test_unserializable_payload_is_rejected_before_touching_the_db(dsn):
    q = Queue(dsn)
    with pytest.raises(TypeError):
        q.enqueue({"when": object()})
    assert q.stats()["queued"] == 0


# ---- arguments and names ----

@pytest.mark.parametrize("name", ["x; DROP TABLE jobs", "has space", "", "a" * 64, "quote'd"])
def test_unsafe_queue_names_are_rejected(dsn, name):
    with pytest.raises(ValueError, match="queue name"):
        Queue(dsn).enqueue({"v": 1}, queue=name)
    with pytest.raises(ValueError, match="queue name"):
        Worker(dsn, noop, queue=name)
    with psycopg.connect(dsn) as conn:  # the table is still there
        assert conn.execute("SELECT count(*) FROM jobs").fetchone()[0] == 0


def test_max_attempts_must_be_positive(dsn):
    with pytest.raises(ValueError, match="max_attempts"):
        Queue(dsn).enqueue({"v": 1}, max_attempts=0)


def test_queues_are_isolated(dsn):
    q = Queue(dsn)
    q.enqueue({"v": 1}, queue="emails")
    q.enqueue({"v": 2}, queue="reports")
    w = Worker(dsn, noop, queue="emails", batch=10)
    assert [j.payload["v"] for j in w.claim()] == [1]
    w.close()


# ---- timing ----

def test_future_run_at_waits_and_past_run_at_runs_now(dsn):
    q = Queue(dsn)
    now = datetime.now(timezone.utc)
    q.enqueue({"v": "later"}, run_at=now + timedelta(seconds=1))
    q.enqueue({"v": "overdue"}, run_at=now - timedelta(days=1))
    w = Worker(dsn, noop, batch=10)
    assert [j.payload["v"] for j in w.claim()] == ["overdue"]
    time.sleep(1.1)
    assert [j.payload["v"] for j in w.claim()] == ["later"]
    w.close()


def test_handler_slower_than_lease_runs_twice_but_effect_once(dsn):
    # Without heartbeats (process() alone), a job that outlives its lease is re-delivered
    # to another worker; the slow worker's late ack is refused; the effect lands once.
    q = Queue(dsn)
    q.enqueue({"value": 1, "sleep": 0.6}, idempotency_key="slow")
    slow = Worker(dsn, record_effect, visibility_timeout=0.3, name="slow")
    fast = Worker(dsn, record_effect, visibility_timeout=30, name="fast")
    [job] = slow.claim()
    t = threading.Thread(target=slow.process, args=(job,))
    t.start()
    time.sleep(0.4)  # lease expired while the handler is still sleeping
    [again] = fast.claim()
    fast.process(again)
    t.join()
    assert effects(dsn) == {"slow": 2}  # ran twice, one row
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT status, locked_by FROM jobs").fetchone() == ("done", None)
    slow.close()
    fast.close()


# ---- concurrency ----

def test_same_idempotency_key_from_many_threads_makes_one_job(dsn):
    ids = []

    def enqueue():
        q = Queue(dsn)
        ids.append(q.enqueue({"v": 1}, idempotency_key="race"))
        q.close()

    threads = [threading.Thread(target=enqueue) for _ in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert len([i for i in ids if i is not None]) == 1
    assert Queue(dsn).stats()["queued"] == 1


def test_empty_queue_worker_idles_and_exits(dsn):
    w = Worker(dsn, noop, poll_interval=0.05)
    start = time.perf_counter()
    assert w.run(idle_exit=0.3) == 0 and time.perf_counter() - start < 2
    w.close()


# ---- infrastructure failures ----

def test_worker_survives_its_database_connections_being_killed(dsn):
    """Simulates a database restart/failover: every worker connection is terminated."""
    q = Queue(dsn)
    q.enqueue_many([{"value": i} for i in range(20)], idempotency_keys=[f"r{i}" for i in range(20)])
    w = Worker(dsn, record_effect, batch=5, poll_interval=0.05, reconnect_delay=0.1)
    t = threading.Thread(target=w.run, kwargs={"idle_exit": 3})
    t.start()
    time.sleep(0.2)
    with psycopg.connect(dsn, autocommit=True) as admin:
        admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = current_database() AND pid <> pg_backend_pid()"
        )
    q2 = Queue(dsn)  # the old producer connection was killed too
    q2.enqueue_many([{"value": i} for i in range(20, 40)], idempotency_keys=[f"r{i}" for i in range(20, 40)])
    t.join(timeout=30)
    assert not t.is_alive()
    assert q2.stats()["done"] == 40 and set(effects(dsn).values()) <= {1, 2}
    w.close()


def test_failing_handler_does_not_poison_the_batch(dsn):
    q = Queue(dsn)
    q.enqueue_many([{"succeed_on": 1}, {"succeed_on": 99}, {"succeed_on": 1}],
                   idempotency_keys=["a", "bad", "c"], max_attempts=1)
    w = Worker(dsn, flaky, batch=10, poll_interval=0.05)
    w.run(idle_exit=0.5)
    assert set(effects(dsn)) == {"a", "c"} and q.stats()["dead"] == 1
    w.close()


def test_heartbeat_keeps_a_slow_but_healthy_job_from_being_redelivered(dsn):
    q = Queue(dsn)
    q.enqueue({"value": 1, "sleep": 1.0}, idempotency_key="long")
    slow = Worker(dsn, record_effect, visibility_timeout=0.3, name="slow")  # heartbeat on by default
    other = Worker(dsn, record_effect, visibility_timeout=30, name="other")
    jobs = slow.claim()
    t = threading.Thread(target=slow.process_batch, args=(jobs,))
    t.start()
    stolen = []
    for _ in range(8):  # for ~0.8 s, well past the 0.3 s lease, keep trying to take it
        time.sleep(0.1)
        stolen += other.claim()
    t.join()
    assert stolen == [] and effects(dsn) == {"long": 1}  # never re-delivered, ran exactly once
    with psycopg.connect(dsn) as conn:
        assert conn.execute("SELECT status, attempts FROM jobs").fetchone() == ("done", 1)
    slow.close()
    other.close()


def test_without_heartbeat_the_same_job_is_redelivered(dsn):
    q = Queue(dsn)
    q.enqueue({"value": 1, "sleep": 1.0}, idempotency_key="long")
    slow = Worker(dsn, record_effect, visibility_timeout=0.3, name="slow", heartbeat=False)
    other = Worker(dsn, noop, visibility_timeout=30, name="other")
    jobs = slow.claim()
    t = threading.Thread(target=slow.process_batch, args=(jobs,))
    t.start()
    time.sleep(0.5)
    assert len(other.claim()) == 1  # lease expired mid-handler: taken again
    t.join()
    slow.close()
    other.close()
