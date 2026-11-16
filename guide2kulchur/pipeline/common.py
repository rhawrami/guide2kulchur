"""Small CLI, configuration, and local-output helpers (no import-time I/O)."""
import argparse
import json
import math
import os
from pathlib import Path
import tempfile

from dotenv import load_dotenv
import psycopg


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('must be a positive integer')
    return number


def nonnegative(value):
    number = float(value)
    if not math.isfinite(number) or number < 0:
        raise argparse.ArgumentTypeError('must be a finite nonnegative number')
    return number


def database(env_file='.env', *, write=False):
    load_dotenv(env_file, override=False)
    conninfo = os.getenv('PG_STRING')
    if not conninfo:
        raise ValueError('Set PG_STRING in the environment or the selected env file')
    conn = psycopg.connect(conninfo, connect_timeout=5, autocommit=True)
    conn.read_only = not write
    if not write:
        # read_only controls explicit psycopg transactions; this also protects
        # individual statements executed while autocommit is enabled.
        conn.execute('SET default_transaction_read_only = on')
    # Fail bounded operations rather than leave an accidental unbounded query running.
    conn.execute("SET statement_timeout = '60s'")
    return conn


def add_database(parser):
    parser.add_argument('--env-file', type=Path, default=Path('.env'))


def add_mode(parser):
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--fetch-only', action='store_true', help='fetch/extract, but do not write the database')
    mode.add_argument('--write', action='store_true', help='explicitly enable network requests and database writes')


def atomic_json(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    # Private mode, same-filesystem atomic replacement; never leave a partial checkpoint.
    fd, temporary = tempfile.mkstemp(dir=path.parent, prefix='.' + path.name)
    try:
        with os.fdopen(fd, 'w') as output:
            json.dump(data, output, ensure_ascii=False, indent=2)
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def output_path(value, *, overwrite=False):
    path = Path(value)
    if path.exists() and not overwrite:
        raise ValueError(f'Output exists: {path}; choose another path or --overwrite')
    path.parent.mkdir(parents=True, exist_ok=True)
    return path
