"""Read-only genre/gender estimates. These inferred binary labels are not identities."""
import argparse
import csv
from pathlib import Path

import plotly.graph_objects as go
from psycopg import sql

from guide2kulchur.pipeline.common import add_database, database, output_path, positive


def population(kind):
    if kind == 'author':
        return sql.SQL('''SELECT p.author_id AS id, p.top_genres AS genres, g.g_comp AS gender
            FROM public.pound p JOIN public.g_pound g USING (author_id)
            WHERE p.descr IS NOT NULL AND p.rating_count >= %s AND p.top_genres[1] IS NOT NULL''')
    return sql.SQL('''SELECT p.user_id AS id, p.favorite_genres AS genres, g.g_nxg AS gender
        FROM public.false_dmitry p JOIN public.g_dmitry g USING (user_id)
        WHERE p.rating_count >= %s AND p.favorite_genres[1] IS NOT NULL''')


def summarize(rows):
    result = []
    for genre, male, female in rows:
        male, female = int(male or 0), int(female or 0)
        total = male + female
        if total:
            result.append((genre, male, female, total, male / total * 100, female / total * 100))
    return result


def render(rows, title, output):
    fig = go.Figure([
        go.Bar(name='Estimated M', x=[r[4] for r in rows], y=[r[0] for r in rows], orientation='h'),
        go.Bar(name='Estimated F', x=[r[5] for r in rows], y=[r[0] for r in rows], orientation='h'),
    ])
    fig.update_layout(barmode='stack', template='plotly_dark', title=title,
                      xaxis_title='Share among classified M/F records (%)')
    fig.write_html(str(output))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--kind', choices=('author', 'user'), required=True)
    parser.add_argument('--min-ratings', type=positive, default=100)
    parser.add_argument('--min-sample', type=positive, default=100)
    parser.add_argument('--limit', type=positive, default=100)
    parser.add_argument('--output-dir', type=Path, default=Path('visualizations/figures'))
    parser.add_argument('--overwrite', action='store_true')
    add_database(parser)
    args = parser.parse_args(argv)
    html = args.output_dir / f'demographics_{args.kind}.html'
    csv_path = html.with_suffix('.csv')
    if not args.overwrite and any(path.exists() for path in (html, csv_path)):
        parser.error('output exists; choose another directory or --overwrite')
    base = population(args.kind)
    query = sql.SQL('''WITH population AS ({}), expanded AS (
        SELECT DISTINCT id, lower(unnest(genres)) AS genre, gender FROM population WHERE gender IN ('M', 'F'))
        SELECT genre, count(*) FILTER (WHERE gender = 'M'), count(*) FILTER (WHERE gender = 'F')
        FROM expanded GROUP BY genre HAVING count(*) >= %s ORDER BY count(*) DESC, genre LIMIT %s''').format(base)
    baseline = sql.SQL('''WITH population AS ({}) SELECT 'OVERALL POPULATION',
        count(*) FILTER (WHERE gender = 'M'), count(*) FILTER (WHERE gender = 'F') FROM population''').format(base)
    with database(args.env_file) as conn:
        with conn.transaction():
            rows = conn.execute(query, (args.min_ratings, args.min_sample, args.limit)).fetchall()
            rows += conn.execute(baseline, (args.min_ratings,)).fetchall()
    rows = summarize(rows)
    output_path(html, overwrite=args.overwrite)
    output_path(csv_path, overwrite=args.overwrite)
    render(rows, f'Goodreads {args.kind}: inferred gender estimates (not self-reported)', html)
    with csv_path.open('w', newline='') as output:
        writer = csv.writer(output)
        writer.writerow(['genre', 'estimated_M', 'estimated_F', 'classified_sample', 'share_M', 'share_F'])
        writer.writerows(rows)
    print(f'{len(rows)} groups exported; unclassified records excluded. Views were not refreshed.')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
