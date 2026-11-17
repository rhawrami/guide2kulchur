"""Read-only live scraper diagnostics. Preview by default; never inserts records."""
import argparse
import asyncio
from collections import Counter
from contextlib import contextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import time
import traceback
import uuid

import aiohttp
from bs4 import BeautifulSoup
from psycopg import sql

from guide2kulchur.pipeline.activity import activity_policy
from guide2kulchur.pipeline.common import add_database, atomic_json, database, nonnegative, positive
from guide2kulchur.pipeline.http import Client, ResponseError
from guide2kulchur.pipeline.runner import _write_records
from guide2kulchur.pipeline.sources import TABLES, normalize_id
from guide2kulchur.privateer.recruits import _rand_headers


class CaptureConnection:
    """Exercise the actual tuple preparation without a database connection."""
    def __init__(self):
        self.rows = []

    @contextmanager
    def cursor(self):
        yield self

    def executemany(self, query, rows):
        rows = list(rows)
        if any(query.count('%s') != len(row) for row in rows):
            raise ValueError('Insert placeholder/parameter mismatch')
        self.rows.extend(rows)


def choose_samples(args, kinds):
    if args.samples:
        selected = json.loads(args.samples.read_text())
    else:
        selected = {}
        with database(args.env_file) as conn:
            conn.execute("SET statement_timeout = '10s'")
            for kind in kinds:
                table, identifier = TABLES[kind]
                minimum = {'book': 1, 'author': 100, 'user': 20}[kind]
                for percent in (0.1, 1, 5):
                    query = sql.SQL('''SELECT {}, rating_count, review_count FROM public.{} TABLESAMPLE SYSTEM ({})
                        WHERE rating_count >= %s ORDER BY random() LIMIT %s''').format(
                        sql.Identifier(identifier), sql.Identifier(table), sql.Literal(percent))
                    rows = conn.execute(query, (minimum, args.count)).fetchall()
                    if len(rows) >= args.count:
                        break
                selected[kind] = [{'id': row[0], 'stored_ratings': row[1], 'stored_reviews': row[2]} for row in rows]
    result = {}
    for kind in kinds:
        seen = set()
        values = []
        if kind == 'book':
            values.extend({'id': value, 'diagnostic_first': True} for value in args.book_first)
        values.extend(selected.get(kind, []))
        result[kind] = []
        for value in values:
            if not isinstance(value, dict) or 'id' not in value:
                raise ValueError('Samples must be mappings of kinds to records with id fields')
            identifier = normalize_id(value['id'], kind)
            if identifier not in seen:
                result[kind].append({**value, 'id': identifier})
                seen.add(identifier)
            if len(result[kind]) == args.count:
                break
        if len(result[kind]) != args.count:
            raise ValueError(f'Need {args.count} distinct {kind} records; found {len(result[kind])}')
    return result


async def probe(kind, samples, args, output, *, client_factory=Client, profile=None):
    report = {'kind': kind, 'target': len(samples), 'records': [],
              'started_at': datetime.now(timezone.utc).isoformat(),
              'request_delay': args.request_delay, 'batch_size': args.batch_size,
              'batch_delay': args.batch_delay, 'concurrency': 1, 'attempts_per_id': 1,
              'max_consecutive_http_ok': 0, 'max_consecutive_pipeline_ok': 0,
              'max_consecutive_eligible_ok': 0,
              'pipeline_success_definition': 'HTTP fetch and parse succeeded; eligible records prepared, inactive records intentionally filtered'}
    trace = aiohttp.TraceConfig()
    current = {}
    in_flight = 0
    max_in_flight = 0
    starts = []
    body = None

    async def started(session, context, params):
        nonlocal in_flight, max_in_flight
        now = time.monotonic()
        starts.append(now)
        current['started_monotonic'] = now
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)

    async def finished(session, context, params):
        nonlocal body, in_flight
        response = params.response
        body = await response.text(errors='replace')
        soup = BeautifulSoup(body, 'lxml')
        current.update(http_status=response.status, final_url=str(response.url),
                       redirects=[{'status': r.status, 'url': str(r.url)} for r in response.history],
                       server=response.headers.get('Server'), waf_action=response.headers.get('x-amzn-waf-action'),
                       retry_after=response.headers.get('Retry-After'),
                       title=soup.title.get_text(' ', strip=True) if soup.title else None,
                       private_profile=soup.find(id='privateProfile') is not None,
                       elapsed_seconds=round(time.monotonic() - current['started_monotonic'], 3),
                       user_agent=response.request_info.headers.get('User-Agent'),
                       accept_language=response.request_info.headers.get('Accept-Language'),
                       cookie_count=len(session.cookie_jar))
        in_flight -= 1

    async def request_error(session, context, params):
        nonlocal in_flight
        in_flight -= 1

    trace.on_request_start.append(started)
    trace.on_request_end.append(finished)
    trace.on_request_exception.append(request_error)
    http_streak = pipeline_streak = eligible_streak = 0
    client = client_factory(concurrency=1, delay=args.request_delay, timeout=args.timeout, trace_configs=[trace])
    if profile:
        client.headers = dict(profile)
    report['profile'] = dict(client.headers)
    async with client:
        for index, sample in enumerate(samples, 1):
            current = {'id': sample['id'], 'sequence': index}
            body = None
            try:
                data = await client.fetch(kind, sample['id'])
                current['parsed_ok'] = True
                current['eligible'] = activity_policy(kind).accepts(data)
                current['counts'] = {key: data.get(key) for key in ['rating_count', 'review_count', 'currently_reading', 'want_to_read']}
                current['null_fields'] = [key for key, value in data.items() if value is None]
                if current['eligible']:
                    capture = CaptureConnection()
                    _write_records(capture, kind, [{'id': sample['id'], 'data': data}], 'success', similar=False)
                    if len(capture.rows) != 1:
                        raise ValueError('Insert preparation did not produce exactly one row')
                    current['prepared_ok'] = True
                current['outcome'] = 'success' if current.get('prepared_ok') else 'filtered'
            except ResponseError as exc:
                current.update(outcome='blocked' if exc.blocked else 'http_error', error=str(exc))
                if exc.blocked:
                    report['cooldown_seconds'] = exc.cooldown
            except Exception as exc:
                current.update(outcome='private' if current.get('private_profile') else 'parse_or_network_error',
                               error=f'{type(exc).__name__}: {exc}', traceback=traceback.format_exc())
            if body is not None:
                (output / f'{kind}_{sample["id"]}.html').write_text(body)
            current.pop('started_monotonic', None)
            http_ok = current.get('http_status') == 200 and current['outcome'] != 'blocked'
            http_streak = http_streak + 1 if http_ok else 0
            pipeline_streak = pipeline_streak + 1 if current['outcome'] in ('success', 'filtered') else 0
            eligible_streak = eligible_streak + 1 if current['outcome'] == 'success' else 0
            report['max_consecutive_http_ok'] = max(report['max_consecutive_http_ok'], http_streak)
            report['max_consecutive_pipeline_ok'] = max(report['max_consecutive_pipeline_ok'], pipeline_streak)
            report['max_consecutive_eligible_ok'] = max(report['max_consecutive_eligible_ok'], eligible_streak)
            report['records'].append(current)
            report['counts'] = dict(Counter(r['outcome'] for r in report['records']))
            report['max_in_flight'] = max_in_flight
            report['requests_started'] = len(starts)
            report['responses_including_redirects'] = sum(1 + len(r.get('redirects', [])) for r in report['records'] if 'http_status' in r)
            report['minimum_start_gap_seconds'] = round(min((b - a for a, b in zip(starts, starts[1:])), default=0), 3)
            atomic_json(output / f'{kind}_report.json', report)
            print(f'{kind} {index:02d}/{len(samples)} {sample["id"]}: HTTP {current.get("http_status", "none")}, {current["outcome"]}', flush=True)
            if client.blocked:
                report['stopped_on_access_restriction'] = True
                break
            if index < len(samples) and index % args.batch_size == 0:
                await asyncio.sleep(args.batch_delay)
    report['finished_at'] = datetime.now(timezone.utc).isoformat()
    atomic_json(output / f'{kind}_report.json', report)
    return report


async def execute(args, kinds, samples, output):
    reports = {}
    # Hold the profile constant for comparisons; each kind gets a fresh session,
    # like a standalone production command. No cross-kind cookie warmup.
    profile = _rand_headers()
    for index, kind in enumerate(kinds):
        report = await probe(kind, samples[kind], args, output, profile=profile)
        reports[kind] = report
        atomic_json(output / 'summary.json', reports)
        if report.get('stopped_on_access_restriction'):
            print('Access restriction; stopping all remaining tests for operator review.', flush=True)
            break
        if index < len(kinds) - 1:
            await asyncio.sleep(60)
    return 0 if len(reports) == len(kinds) and all(r['max_consecutive_pipeline_ok'] == args.count for r in reports.values()) else 1


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind', choices=('all', 'book', 'author', 'user'), default='all')
    parser.add_argument('--count', type=positive, default=50)
    parser.add_argument('--request-delay', type=nonnegative, default=5,
                        help='seconds between record-fetch starts; canonical redirects are automatic')
    parser.add_argument('--batch-size', type=positive, default=10)
    parser.add_argument('--batch-delay', type=nonnegative, default=15)
    parser.add_argument('--timeout', type=positive, default=30)
    parser.add_argument('--samples', type=Path, help='reuse a saved samples.json instead of querying the DB')
    parser.add_argument('--book-first', nargs='*', default=[], help='diagnostic book IDs to put first (included in the count)')
    parser.add_argument('--output-dir', type=Path)
    parser.add_argument('--execute', action='store_true', help='enable live HTTP; NEVER writes the database')
    add_database(parser)
    args = parser.parse_args(argv)
    kinds = ['author', 'user', 'book'] if args.kind == 'all' else [args.kind]
    samples = choose_samples(args, kinds)
    if not args.execute:
        print(json.dumps({'mode': 'preview', 'samples': samples}, indent=2))
        return 0
    output = args.output_dir or Path('logs/smoke') / uuid.uuid4().hex
    output.mkdir(parents=True, exist_ok=False)
    output.chmod(0o700)
    atomic_json(output / 'samples.json', samples)
    return asyncio.run(execute(args, kinds, samples, output))


if __name__ == '__main__':
    raise SystemExit(main())
