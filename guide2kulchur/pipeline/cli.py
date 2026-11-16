"""Command-line collection: preview by default, fetch/write only when requested."""
import argparse
import asyncio
from datetime import date
import json
from pathlib import Path
import sys

import psycopg

from .common import add_database, add_mode, database, nonnegative, positive
from .runner import load_checkpoint, pending, run
from .sources import SOURCES, discover, file_ids, missing_ids, similar_ids


def parser():
    root = argparse.ArgumentParser(description=__doc__)
    commands = root.add_subparsers(dest='command', required=True)
    for name in ('collect', 'similar'):
        cmd = commands.add_parser(name, help='collect records' if name == 'collect' else 'fill recommendation arrays')
        cmd.add_argument('--kind', choices=('book', 'author', 'user') if name == 'collect' else ('book', 'author'), required=True)
        source = cmd.add_mutually_exclusive_group(required=name == 'collect')
        if name == 'collect':
            source.add_argument('--ids', nargs='+', help='Goodreads IDs or HTTPS URLs')
            source.add_argument('--ids-file', type=Path, help='text/gzip or saved genre/awards JSON')
            source.add_argument('--source', choices=sorted(SOURCES))
        source.add_argument('--resume', type=Path, help='resume a local checkpoint with the same operation/write mode')
        cmd.add_argument('--active-since', type=date.fromisoformat, help='filter user-* discovery sources by reading update date')
        cmd.add_argument('--min-ratings', type=int, default=0, help='exclude fetched records below this rating count (unknown counts treated as zero)')
        cmd.add_argument('--min-reviews', type=int, default=0, help='exclude fetched records below this review count')
        cmd.add_argument('--batches', type=positive, default=1)
        cmd.add_argument('--batch-size', type=positive, default=10)
        cmd.add_argument('--concurrency', type=positive, default=1)
        cmd.add_argument('--request-delay', type=nonnegative, default=2, help='seconds between request starts, globally')
        cmd.add_argument('--batch-delay', type=nonnegative, default=10)
        cmd.add_argument('--attempts', type=positive, default=2, help='bounded retries for network/5xx errors, never challenges')
        cmd.add_argument('--timeout', type=positive, default=30)
        cmd.add_argument('--backoff', type=nonnegative, default=15)
        cmd.add_argument('--user-agent', help='explicit profile override; default: one existing randomized profile per session')
        cmd.add_argument('--checkpoint', type=Path, help='new checkpoint path (default: logs/runs/<run-id>/checkpoint.json)')
        add_database(cmd)
        add_mode(cmd)
    return root


def main(argv=None):
    cli = parser()
    args = cli.parse_args(argv)
    if args.min_ratings < 0 or args.min_reviews < 0:
        cli.error('--min-ratings and --min-reviews must be nonnegative')
    if args.command == 'similar' and (args.min_ratings or args.min_reviews):
        cli.error('activity thresholds apply to collect, not similar')
    if args.active_since and not (getattr(args, 'source', '') or '').startswith('user-'):
        cli.error('--active-since requires a user-* discovery source')
    if args.resume and args.checkpoint:
        cli.error('--resume and --checkpoint cannot be combined')
    if getattr(args, 'source', None) and SOURCES[args.source][0] != args.kind:
        cli.error(f'{args.source} discovers {SOURCES[args.source][0]} records')
    try:
        if args.resume:
            state = load_checkpoint(args.resume, args.kind, args.command == 'similar', args.write)
            if (state.get('min_ratings', 0), state.get('min_reviews', 0)) != (args.min_ratings, args.min_reviews):
                raise ValueError('Checkpoint activity thresholds differ; supply the original thresholds')
        with database(args.env_file, write=args.write) as conn:
            if not args.resume:
                limit = args.batches * args.batch_size
                if args.command == 'similar':
                    ids = similar_ids(conn, args.kind, limit)
                elif args.source:
                    ids = discover(conn, args.kind, args.source, limit, args.active_since)
                else:
                    values = args.ids if args.ids is not None else file_ids(args.ids_file)
                    ids = missing_ids(conn, args.kind, values, limit)
                state = {'version': 1, 'kind': args.kind, 'similar': args.command == 'similar',
                         'write': args.write, 'min_ratings': args.min_ratings, 'min_reviews': args.min_reviews,
                         'ids': ids, 'results': {}}
            if not (args.fetch_only or args.write):
                print(json.dumps({'mode': 'preview', 'kind': args.kind, 'operation': args.command,
                                  'ids': pending(state)[:args.batches * args.batch_size]}, indent=2))
                return 0
            if not pending(state):
                print('No pending IDs.')
                return 0
            return asyncio.run(run(conn, args, state))
    except psycopg.Error as exc:
        # Do not leak a password-bearing conninfo string in diagnostics.
        print(f'Database operation failed ({type(exc).__name__}); check local configuration and schema.', file=sys.stderr)
        return 1
    except (ValueError, OSError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print('Interrupted; committed batches remain checkpointed. Pending work can be resumed.', file=sys.stderr)
        return 130


if __name__ == '__main__':
    raise SystemExit(main())
