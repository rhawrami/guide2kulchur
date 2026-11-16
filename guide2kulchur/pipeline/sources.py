"""Bounded discovery without destructive staging tables."""
import gzip
import json
from pathlib import Path
import re
from urllib.parse import urlparse

from psycopg import sql

TABLES = {'book': ('alexandria', 'book_id'), 'author': ('pound', 'author_id'), 'user': ('false_dmitry', 'user_id')}
# source: target kind, source table, source column, array?
SOURCES = {
    'similar-books': ('book', 'alexandria', 'sim_books', True),
    'book-authors': ('author', 'alexandria', 'author_id', False),
    'similar-authors': ('author', 'pound', 'sim_authors', True),
    'author-books': ('book', 'pound', 'book_sample', True),
    'user-friends': ('user', 'false_dmitry', 'friends_sample', True),
    'user-following-users': ('user', 'false_dmitry', 'followings_sample_users', True),
    'user-reading-books': ('book', 'false_dmitry', 'currently_reading_sample_books', True),
    'user-shelf-books': ('book', 'false_dmitry', 'featured_shelf_sample_books', True),
    'user-reading-authors': ('author', 'false_dmitry', 'currently_reading_sample_authors', True),
    'user-following-authors': ('author', 'false_dmitry', 'followings_sample_authors', True),
    'user-quoted-authors': ('author', 'false_dmitry', 'quotes_sample_author_ids', True),
}


def normalize_id(value, kind):
    value = str(value).strip()
    if re.fullmatch(r'[0-9]+', value):
        return str(int(value))
    parsed = urlparse(value)
    match = re.match(rf'^/{kind}/show/([0-9]+)(?:[.\-/]|$)', parsed.path)
    if parsed.scheme == 'https' and parsed.hostname in ('goodreads.com', 'www.goodreads.com') and match:
        return str(int(match.group(1)))
    raise ValueError(f'Expected a {kind} ID or Goodreads URL, got {value[:100]!r}')


def file_ids(path):
    path = Path(path)
    opener = gzip.open if path.suffix == '.gz' else open
    with opener(path, 'rt') as stream:
        if '.json' in path.suffixes:
            data = json.load(stream)
            results = data.get('results', data) if isinstance(data, dict) else data
            if isinstance(results, dict):
                for values in results.values():
                    if isinstance(values, list):
                        yield from values
            elif isinstance(results, list):
                for value in results:
                    yield value['id'] if isinstance(value, dict) else value
            else:
                raise ValueError('Expected a JSON list or results mapping')
        else:
            for line in stream:
                value = line.strip()
                if value and not value.startswith('#'):
                    yield value


def missing_ids(conn, kind, values, limit):
    """Stream files in small chunks; do not load multi-million-line sitemaps at once."""
    table, column = TABLES[kind]
    selected, chunk = [], []

    def filter_chunk():
        query = sql.SQL('''SELECT input.id FROM unnest(%s::text[]) WITH ORDINALITY AS input(id, n)
            WHERE NOT EXISTS (SELECT 1 FROM public.{} t WHERE t.{} = input.id)
            AND NOT EXISTS (SELECT 1 FROM public.error_id e WHERE e.item_id = input.id AND e.item_type = %s)
            ORDER BY input.n''').format(sql.Identifier(table), sql.Identifier(column))
        return [row[0] for row in conn.execute(query, (chunk, kind)).fetchall()]

    for value in values:
        item = normalize_id(value, kind)
        if item in chunk or item in selected:
            continue
        chunk.append(item)
        if len(chunk) >= min(1000, limit):
            selected.extend(filter_chunk())
            chunk.clear()
            if len(selected) >= limit:
                return selected[:limit]
    if chunk:
        selected.extend(filter_chunk())
    return selected[:limit]


def discover(conn, kind, source, limit, active_since=None):
    target, table, column, array = SOURCES[source]
    if target != kind:
        raise ValueError(f'{source} discovers {target} records, not {kind}')
    if active_since and table != 'false_dmitry':
        raise ValueError('--active-since applies only to user-* sources')
    expression = sql.SQL('unnest({})' if array else '{}').format(sql.Identifier(column))
    condition = sql.SQL('WHERE cr_recent_update >= %s') if active_since else sql.SQL('')
    target_table, target_column = TABLES[kind]
    query = sql.SQL('''WITH candidates AS (SELECT DISTINCT {} AS id FROM public.{} {})
        SELECT id FROM candidates c WHERE id ~ '^[0-9]+$'
        AND NOT EXISTS (SELECT 1 FROM public.{} t WHERE t.{} = c.id)
        AND NOT EXISTS (SELECT 1 FROM public.error_id e WHERE e.item_id = c.id AND e.item_type = %s)
        ORDER BY id LIMIT %s''').format(expression, sql.Identifier(table), condition,
                                      sql.Identifier(target_table), sql.Identifier(target_column))
    params = ([active_since] if active_since else []) + [kind, limit]
    return [row[0] for row in conn.execute(query, params).fetchall()]


def similar_ids(conn, kind, limit):
    if kind == 'book':
        query = '''SELECT DISTINCT sim_books_url_id FROM public.alexandria
            WHERE sim_books IS NULL AND sim_books_url_id ~ '^[0-9]+$'
            ORDER BY sim_books_url_id LIMIT %s'''
    else:
        query = '''SELECT author_id FROM public.pound WHERE sim_authors IS NULL
            ORDER BY author_id LIMIT %s'''
    return [row[0] for row in conn.execute(query, (limit,)).fetchall()]
