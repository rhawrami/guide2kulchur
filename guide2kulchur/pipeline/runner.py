"""Bounded batches, explicit writes, and durable local checkpoints."""
import asyncio
from collections import Counter
import json
import logging
from pathlib import Path
import uuid

import aiohttp
import psycopg

from guide2kulchur.engineer.batchpullers import BatchBookPuller, BatchAuthorPuller, BatchUserPuller
from .common import atomic_json
from .http import Client, ResponseError


TERMINAL = {'success', 'missing', 'filtered'}
INSERT_CLASSES = {'book': (BatchBookPuller, 'book_ids'), 'author': (BatchAuthorPuller, 'author_ids'),
                  'user': (BatchUserPuller, 'user_ids')}


def load_checkpoint(path, kind, similar, write):
    state = json.loads(Path(path).read_text())
    if (state.get('version'), state.get('kind'), state.get('similar'), state.get('write')) != (1, kind, similar, write):
        raise ValueError('Checkpoint version/kind/operation/write mode does not match this run')
    ids = state.get('ids')
    if not isinstance(ids, list) or any(not isinstance(item, str) or not item.isascii() or not item.isdigit() for item in ids):
        raise ValueError('Invalid checkpoint IDs')
    if not isinstance(state.get('results'), dict):
        raise ValueError('Invalid checkpoint results')
    return state


def pending(state):
    return [item for item in state['ids'] if state['results'].get(item, {}).get('status') not in TERMINAL]


def persist(conn, kind, records, *, similar=False):
    """Commit a batch, with per-record savepoints. Never blacklist parsing/transient errors."""
    logger = logging.getLogger('pipeline.insert')
    with conn.transaction():
        for record in records:
            if record['status'] not in ('success', 'missing'):
                continue
            # A missing *recommendations page* does not imply a missing entity.
            if record['status'] == 'missing' and similar:
                record['status'] = 'failed'
                continue
            try:
                with conn.transaction():
                    if record['status'] == 'missing':
                        conn.execute('INSERT INTO public.error_id (item_id, item_type) VALUES (%s, %s) ON CONFLICT DO NOTHING',
                                     (record['id'], kind))
                    elif similar:
                        if kind == 'book':
                            query = 'UPDATE public.alexandria SET sim_books = %s WHERE sim_books_url_id = %s'
                        else:
                            query = 'UPDATE public.pound SET sim_authors = %s WHERE author_id = %s'
                        conn.execute(query, (record['data'], record['id']))
                    else:
                        cls, argument = INSERT_CLASSES[kind]
                        with conn.cursor() as cursor:
                            batch = cls(batch_id='cli', cursor=cursor, semaphore_count=1,
                                        status_logger=logger, **{argument: [record['id']]})
                            batch.successes = [record['data']]
                            batch.insert_batch_into_db()
            except (psycopg.Error, ValueError, KeyError, TypeError) as exc:
                record['status'] = 'db_error'
                record['error'] = type(exc).__name__


async def run(conn, args, state, *, client_factory=Client):
    path = args.resume or args.checkpoint or Path('logs/runs') / uuid.uuid4().hex / 'checkpoint.json'
    path = Path(path)
    if path.exists() and not args.resume:
        raise ValueError('Checkpoint already exists; use --resume or choose another path')
    atomic_json(path, state)
    remaining = pending(state)[:args.batches * args.batch_size]
    semaphore = asyncio.Semaphore(args.concurrency)
    async with client_factory(concurrency=args.concurrency, delay=args.request_delay,
                              timeout=args.timeout, user_agent=args.user_agent) as client:
        async def one(identifier):
            async with semaphore:
                if client.blocked:
                    return {'id': identifier, 'status': 'pending'}
                for attempt in range(args.attempts):
                    try:
                        data = await client.fetch(args.kind, identifier, similar=args.command == 'similar')
                        if args.command != 'similar' and ((data.get('rating_count') or 0) < args.min_ratings
                                                          or (data.get('review_count') or 0) < args.min_reviews):
                            return {'id': identifier, 'status': 'filtered', 'reason': 'below activity thresholds'}
                        return {'id': identifier, 'status': 'success', 'data': data}
                    except ResponseError as exc:
                        if exc.blocked:
                            return {'id': identifier, 'status': 'blocked', 'error': str(exc), 'cooldown_seconds': exc.cooldown}
                        if exc.missing:
                            return {'id': identifier, 'status': 'failed' if args.command == 'similar' else 'missing'}
                        if exc.status < 500:
                            return {'id': identifier, 'status': 'failed', 'error': str(exc)}
                        error = str(exc)
                    except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                        error = type(exc).__name__
                    except Exception as exc:
                        # Parser failures are visible and retryable on a later run, never invalid IDs.
                        return {'id': identifier, 'status': 'failed', 'error': type(exc).__name__ + ': ' + str(exc)[:300]}
                    if attempt + 1 < args.attempts and not client.blocked:
                        await asyncio.sleep(args.backoff * 2 ** attempt)
                    else:
                        break
                return {'id': identifier, 'status': 'failed', 'error': error}

        for offset in range(0, len(remaining), args.batch_size):
            # No next batch is scheduled after a challenge. In-flight requests may finish.
            records = await asyncio.gather(*(one(item) for item in remaining[offset:offset + args.batch_size]))
            if args.write:
                persist(conn, args.kind, records, similar=args.command == 'similar')
            for record in records:
                state['results'][record['id']] = {k: v for k, v in record.items() if k != 'data'}
            # Only acknowledge completion after the DB transaction committed.
            atomic_json(path, state)
            atomic_json(path.with_name('last_batch.json'), records)
            print(json.dumps({'batch': offset // args.batch_size + 1,
                              'counts': dict(Counter(r['status'] for r in records)), 'checkpoint': str(path)}))
            if client.blocked:
                print(f'Stopped: {client.blocked}. Wait at least {client.blocked.cooldown:.0f}s and review access before --resume.')
                return 2
            if offset + args.batch_size < len(remaining):
                await asyncio.sleep(args.batch_delay)
    return 1 if any(r.get('status') not in TERMINAL for r in state['results'].values()) else 0
