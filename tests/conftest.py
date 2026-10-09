"""Test fixtures: a throwaway PostgreSQL cluster per test session.

initdb creates a fresh cluster in a temp directory, pg_ctl starts it on a free
port, and it's deleted afterwards. Tests run against real Postgres (row locks,
SKIP LOCKED and NOTIFY behave exactly as in production) without touching any
database you already have. Set JOBQUEUE_TEST_DSN to use an existing server instead.
"""

import os
import shutil
import socket
import subprocess
import tempfile
import uuid
from pathlib import Path

import psycopg
import pytest

from jobqueue.examples import EFFECTS_SQL
from jobqueue.queue import init_db


def pg_bin(name: str) -> str:
    for candidate in (shutil.which(name), f"/opt/homebrew/opt/postgresql@17/bin/{name}",
                      f"/usr/lib/postgresql/17/bin/{name}"):
        if candidate and Path(candidate).exists():
            return candidate
    pytest.skip(f"PostgreSQL binary {name!r} not found")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def server_dsn():
    if os.environ.get("JOBQUEUE_TEST_DSN"):
        yield os.environ["JOBQUEUE_TEST_DSN"]
        return
    data = Path(tempfile.mkdtemp(prefix="jobqueue-pg-"))
    port = free_port()
    subprocess.run([pg_bin("initdb"), "-D", str(data), "-U", "postgres", "--auth=trust", "-E", "UTF8"],
                   check=True, capture_output=True)
    subprocess.run([pg_bin("pg_ctl"), "-D", str(data), "-l", str(data / "log"), "-w", "start",
                    "-o", f"-p {port} -k {data} -c listen_addresses=127.0.0.1 -c max_connections=200 -c fsync=on"],
                   check=True, capture_output=True)
    try:
        yield f"postgresql://postgres@127.0.0.1:{port}/postgres"
    finally:
        subprocess.run([pg_bin("pg_ctl"), "-D", str(data), "-m", "immediate", "stop"], capture_output=True)
        shutil.rmtree(data, ignore_errors=True)


@pytest.fixture
def dsn(server_dsn):
    """A fresh database per test, with the queue schema and the example effects table."""
    name = f"t_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(server_dsn, autocommit=True) as conn:
        conn.execute(f"CREATE DATABASE {name}")
    test_dsn = server_dsn.rsplit("/", 1)[0] + f"/{name}"
    init_db(test_dsn)
    with psycopg.connect(test_dsn, autocommit=True) as conn:
        conn.execute(EFFECTS_SQL)
    yield test_dsn
    with psycopg.connect(server_dsn, autocommit=True) as conn:
        conn.execute(f"DROP DATABASE {name} WITH (FORCE)")
