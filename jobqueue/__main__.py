"""Command line.

    python -m jobqueue init-db  --dsn postgresql://...
    python -m jobqueue worker   --dsn postgresql://... --handler jobqueue.examples:record_effect
"""

from __future__ import annotations

import argparse
import importlib
import logging
import os

from jobqueue.queue import init_db
from jobqueue.worker import run_worker


def load_handler(spec: str):
    module, _, name = spec.partition(":")
    return getattr(importlib.import_module(module), name)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="jobqueue")
    sub = parser.add_subparsers(dest="cmd", required=True)
    for name in ("init-db", "worker"):
        p = sub.add_parser(name)
        p.add_argument("--dsn", default=os.environ.get("DATABASE_URL"), required="DATABASE_URL" not in os.environ)
    w = sub.choices["worker"]
    w.add_argument("--handler", default="jobqueue.examples:record_effect")
    w.add_argument("--queue", default="default")
    w.add_argument("--batch", type=int, default=1)
    w.add_argument("--visibility-timeout", type=float, default=30.0)
    w.add_argument("--poll-interval", type=float, default=1.0)
    w.add_argument("--no-skip-locked", action="store_true", help="benchmark: plain FOR UPDATE claims")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    if args.cmd == "init-db":
        init_db(args.dsn)
        return
    run_worker(
        args.dsn,
        load_handler(args.handler),
        queue=args.queue,
        batch=args.batch,
        visibility_timeout=args.visibility_timeout,
        poll_interval=args.poll_interval,
        skip_locked=not args.no_skip_locked,
    )


if __name__ == "__main__":
    main()
