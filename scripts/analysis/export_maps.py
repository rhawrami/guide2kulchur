"""Read-only CSV map exports with bounded, configurable filters."""
import argparse
import csv
from datetime import date
from pathlib import Path

from psycopg import sql

from guide2kulchur.pipeline.common import add_database, database, output_path, positive


def query_for(kind):
    if kind == 'book':
        fields = sql.SQL('b.book_id, b.title, p.author_name, b.first_published, b.rating, b.rating_count, b.top_genres')
        source = sql.SQL('public.alexandria b JOIN public.pound p ON b.author_id = p.author_id')
        alias, date_col = sql.Identifier('b'), sql.SQL('b.first_published')
    else:
        fields = sql.SQL('p.author_id, p.author_name, p.author_name, p.birth, p.rating, p.rating_count, p.top_genres')
        source = sql.SQL('public.pound p')
        alias, date_col = sql.Identifier('p'), sql.SQL('p.birth')
    return sql.SQL('''SELECT {}, l.addr, l.lat, l.lon FROM {}
        JOIN public.birth_place_locs l ON lower(p.birth_place) = l.og_loc
        WHERE l.lat IS NOT NULL AND l.lon IS NOT NULL AND {}.rating_count >= %s
        AND (%s::text IS NULL OR l.addr ILIKE %s)
        AND (%s::text IS NULL OR EXISTS (SELECT 1 FROM unnest({}.top_genres) g WHERE g ILIKE %s))
        AND (%s::date IS NULL OR {} >= %s)
        ORDER BY {}.rating_count DESC LIMIT %s''').format(fields, source, alias, alias, date_col, alias)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind', choices=('book', 'author'), required=True)
    parser.add_argument('--location', help='address substring, e.g. Japan')
    parser.add_argument('--genre', help='genre LIKE pattern, e.g. %%poetry%%')
    parser.add_argument('--since', type=date.fromisoformat, help='publication date (book) or birth date (author)')
    parser.add_argument('--min-ratings', type=positive, default=100)
    parser.add_argument('--limit', type=positive, default=10000)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--overwrite', action='store_true')
    add_database(parser)
    args = parser.parse_args(argv)
    if args.output.exists() and not args.overwrite:
        parser.error('output exists; choose another path or --overwrite')
    params = (args.min_ratings, args.location, f'%{args.location}%' if args.location else None,
              args.genre, args.genre, args.since, args.since, args.limit)
    with database(args.env_file) as conn:
        rows = conn.execute(query_for(args.kind), params).fetchall()
    path = output_path(args.output, overwrite=args.overwrite)
    with path.open('w', newline='') as output:
        writer = csv.writer(output)
        writer.writerow(['id', 'name', 'author', 'event_date', 'rating', 'rating_count', 'genres', 'address', 'latitude', 'longitude'])
        writer.writerows(rows)
    print(f'{len(rows)} rows -> {path}')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
