"""Bounded batches, explicit writes, and durable local checkpoints."""
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import logging
from pathlib import Path
import uuid

import aiohttp
import psycopg

from guide2kulchur.engineer.batchpullers import BatchBookPuller, BatchAuthorPuller, BatchUserPuller
from .activity import activity_policy
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
    if not isinstance(state.get('results'), dict) or any(
            not isinstance(value, dict) for value in state['results'].values()):
        raise ValueError('Invalid checkpoint results')
    return state


def pending(state):
    return list(dict.fromkeys(item for item in state['ids']
                              if state['results'].get(item, {}).get('status') not in TERMINAL))


ROW_ERRORS = (psycopg.DataError, psycopg.IntegrityError, ValueError, KeyError, TypeError)


def _write_records(conn, kind, records, status, *, similar):
    """Prepare and submit a whole operation group through one executemany call."""
    with conn.cursor() as cursor:
        if status == 'missing':
            cursor.executemany(
                'INSERT INTO public.error_id (item_id, item_type) VALUES (%s, %s) ON CONFLICT DO NOTHING',
                [(record['id'], kind) for record in records])
        elif similar:
            query = ('UPDATE public.alexandria SET sim_books = %s WHERE sim_books_url_id = %s'
                     if kind == 'book' else
                     'UPDATE public.pound SET sim_authors = %s WHERE author_id = %s')
            cursor.executemany(query, [(record['data'], record['id']) for record in records])
        else:
            cls, argument = INSERT_CLASSES[kind]
            batch = cls(batch_id='cli', cursor=cursor, semaphore_count=1,
                        status_logger=logging.getLogger('pipeline.insert'),
                        **{argument: [record['id'] for record in records]})
            batch.successes = [record['data'] for record in records]
            batch.insert_batch_into_db()


def persist(conn, kind, records, *, similar=False):
    """Commit once per batch; isolate rows only if a bulk operation fails."""
    by_id = {}
    for record in records:
        if record['status'] not in ('success', 'missing'):
            continue
        # A missing recommendations page does not imply a missing entity.
        if record['status'] == 'missing' and similar:
            record['status'] = 'failed'
            continue
        by_id.setdefault(record['id'], []).append(record)
    if not by_id:
        return

    groups = {'success': [], 'missing': []}
    for duplicates in by_id.values():
        # If duplicate outcomes disagree, a successful fetch overrides a 404.
        record = next((r for r in duplicates if r['status'] == 'success'), duplicates[0])
        groups[record['status']].append((record, duplicates))

    with conn.transaction():
        for status, group in groups.items():
            if not group:
                continue
            selected = [record for record, _ in group]
            try:
                # Roll back the entire bulk attempt before retrying individual rows.
                with conn.transaction():
                    _write_records(conn, kind, selected, status, similar=similar)
            except ROW_ERRORS as exc:
                logging.getLogger('pipeline.insert').warning(
                    'Bulk %s failed (%s); isolating %s records', status, type(exc).__name__, len(selected))
                for record in selected:
                    try:
                        with conn.transaction():
                            _write_records(conn, kind, [record], status, similar=similar)
                    except ROW_ERRORS as row_error:
                        record['status'] = 'db_error'
                        record['error'] = type(row_error).__name__
            # Connection/schema failures propagate: don't acknowledge an uncommitted batch.
            for record, duplicates in group:
                for duplicate in duplicates:
                    duplicate['status'] = record['status']
                    if record['status'] == 'db_error':
                        duplicate['error'] = record['error']


async def run(conn, args, state, *, client_factory=Client):
    path = args.resume or args.checkpoint or Path('logs/runs') / uuid.uuid4().hex / 'checkpoint.json'
    path = Path(path)
    if path.exists() and not args.resume:
        raise ValueError('Checkpoint already exists; use --resume or choose another path')
    atomic_json(path, state)
    remaining = pending(state)[:args.batches * args.batch_size]
    semaphore = asyncio.Semaphore(args.concurrency)
    policy = activity_policy(args.kind, args.min_ratings, args.min_reviews, args.activity_policy)
    async with client_factory(concurrency=args.concurrency, delay=args.request_delay,
                              timeout=args.timeout, user_agent=args.user_agent) as client:
        headers = getattr(client, 'headers', {})
        state.setdefault('http_profiles', []).append({
            'started_at': datetime.now(timezone.utc).isoformat(),
            'headers': {key: headers[key] for key in ('User-Agent', 'Accept-Language', 'DNT') if key in headers},
        })
        atomic_json(path, state)
        async def one(identifier):
            async with semaphore:
                if client.blocked:
                    return {'id': identifier, 'status': 'pending'}
                for attempt in range(args.attempts):
                    try:
                        data = await client.fetch(args.kind, identifier, similar=args.command == 'similar')
                        if args.command != 'similar' and not policy.accepts(data):
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
    print(json.dumps({'pending': len(pending(state)), 'discovery': state.get('discovery', {}), 'checkpoint': str(path)}))
    return 1 if any(r.get('status') not in TERMINAL for r in state['results'].values()) else 0
