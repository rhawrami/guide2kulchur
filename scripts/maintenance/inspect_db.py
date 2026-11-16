"""Read-only database inventory, optional bounded sample or private schema snapshot."""
import argparse
import json
import os
from pathlib import Path
import subprocess
import tempfile

from psycopg import sql
from psycopg.conninfo import conninfo_to_dict
from psycopg.pq import Conninfo

from guide2kulchur.pipeline.common import add_database, database, positive
from guide2kulchur.pipeline.sources import TABLES


def dump_schema(output, overwrite):
    if output.exists() and not overwrite:
        raise ValueError('Schema output exists; use --overwrite explicitly')
    output.parent.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    names = {item.keyword.decode(): item.envvar.decode() for item in Conninfo.get_defaults() if item.envvar}
    for key, value in conninfo_to_dict(os.environ['PG_STRING']).items():
        if key not in names:
            raise ValueError(f'pg_dump environment mapping unavailable for connection option {key}')
        env[names[key]] = value
    fd, temporary = tempfile.mkstemp(dir=output.parent, prefix='.schema-')
    try:
        with os.fdopen(fd, 'w') as stream:
            subprocess.run(['pg_dump', '--schema-only', '--no-owner', '--no-privileges', '--no-password'],
                           env=env, stdout=stream, check=True, timeout=60)
        os.replace(temporary, output)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--sample', choices=('book', 'author', 'user'))
    parser.add_argument('--limit', type=positive, default=1)
    parser.add_argument('--schema-output', type=Path, help='save a private schema-only dump, normally .schema_local')
    parser.add_argument('--overwrite', action='store_true')
    add_database(parser)
    args = parser.parse_args(argv)
    with database(args.env_file) as conn:
        if args.sample:
            table, column = TABLES[args.sample]
            name = {'book': 'title', 'author': 'author_name', 'user': 'user_name'}[args.sample]
            query = sql.SQL('SELECT {}, {} FROM public.{} LIMIT %s').format(sql.Identifier(column), sql.Identifier(name), sql.Identifier(table))
            print(json.dumps(conn.execute(query, (args.limit,)).fetchall(), ensure_ascii=False))
        else:
            query = '''SELECT relname, reltuples::bigint AS estimated_rows FROM pg_class c
                JOIN pg_namespace n ON n.oid = c.relnamespace
                WHERE n.nspname = 'public' AND relkind IN ('r', 'm') ORDER BY relname'''
            print(json.dumps(conn.execute(query).fetchall(), indent=2))
        if args.schema_output:
            dump_schema(args.schema_output, args.overwrite)
            print(f'Saved {args.schema_output}; never commit private schema snapshots.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
