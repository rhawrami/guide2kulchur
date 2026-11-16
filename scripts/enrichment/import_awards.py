"""Load saved Choice Awards JSON. Preview by default; no network requests."""
import argparse
import itertools
import json
from pathlib import Path

from guide2kulchur.pipeline.common import add_database, database, positive

INSERT = '''INSERT INTO public.gr_awards (book_id, author_id, award_year, award_category, award_num_votes)
    SELECT %s, %s, %s, %s, %s WHERE NOT EXISTS (
      SELECT 1 FROM public.gr_awards WHERE book_id = %s AND award_year = %s AND award_category = %s)'''


def records(paths):
    seen = set()
    for path in paths:
        data = json.loads(Path(path).read_text())
        for row in data['results']:
            record = (str(row['id']), str(row['author_id']), int(row['award_year']),
                      str(row['award_category']), str(row['award_num_votes']))
            key = record[0], record[2], record[3]
            if key not in seen:
                seen.add(key)
                yield record


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--input', nargs='+', type=Path, required=True)
    parser.add_argument('--limit', type=positive, default=1000)
    parser.add_argument('--batch-size', type=positive, default=100)
    parser.add_argument('--write', action='store_true')
    add_database(parser)
    args = parser.parse_args(argv)
    rows = list(itertools.islice(records(args.input), args.limit))
    if args.write:
        with database(args.env_file, write=True) as conn:
            for offset in range(0, len(rows), args.batch_size):
                with conn.transaction(), conn.cursor() as cur:
                    cur.executemany(INSERT, [row + (row[0], row[2], row[3]) for row in rows[offset:offset + args.batch_size]])
    print(json.dumps({'records': len(rows), 'mode': 'write' if args.write else 'preview'}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
