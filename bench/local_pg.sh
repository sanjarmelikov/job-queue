#!/usr/bin/env bash
# Start/stop a local throwaway Postgres for benchmarks (data in .pgdata/, port 5499).
#   bench/local_pg.sh start   → prints the DSN
#   bench/local_pg.sh stop
set -euo pipefail
cd "$(dirname "$0")/.."
PGBIN="${PGBIN:-/opt/homebrew/opt/postgresql@17/bin}"
DATA=.pgdata PORT=5499
case "${1:-}" in
  start)
    [ -d "$DATA" ] || "$PGBIN/initdb" -D "$DATA" -U postgres --auth=trust -E UTF8 >/dev/null
    "$PGBIN/pg_ctl" -D "$DATA" -l "$DATA/log" -w start \
      -o "-p $PORT -k $PWD/$DATA -c listen_addresses=127.0.0.1 -c max_connections=200" >/dev/null
    DSN="postgresql://postgres@127.0.0.1:$PORT/postgres"
    .venv/bin/python -c "
import psycopg
from jobqueue.queue import init_db
from jobqueue.examples import EFFECTS_SQL
init_db('$DSN')
with psycopg.connect('$DSN', autocommit=True) as c: c.execute(EFFECTS_SQL)"
    echo "$DSN" ;;
  stop) "$PGBIN/pg_ctl" -D "$DATA" -m fast stop >/dev/null && echo stopped ;;
  *) echo "usage: $0 start|stop" >&2; exit 2 ;;
esac
