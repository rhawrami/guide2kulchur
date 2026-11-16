"""Fetch bounded Wikidata biographies into wikidata_lb, not directly into pound."""
import argparse
import json

import requests

from guide2kulchur.pipeline.common import add_database, add_mode, database, positive

QUERY = '''PREFIX wd: <http://www.wikidata.org/entity/>
PREFIX wdt: <http://www.wikidata.org/prop/direct/>
PREFIX wikibase: <http://wikiba.se/ontology#>
PREFIX bd: <http://www.bigdata.com/rdf#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
SELECT DISTINCT ?author ?authorLabel ?nativeName ?dob ?dod ?pob ?pobLabel WHERE {
  ?author wdt:P2963 ?goodreadsId .
  OPTIONAL { ?author wdt:P569 ?dob. }
  OPTIONAL { ?author wdt:P570 ?dod. }
  OPTIONAL { ?author wdt:P19 ?pob. }
  OPTIONAL { ?author wdt:P1559 ?nativeName. }
  FILTER (?dob >= "START-01-01T00:00:00Z"^^xsd:dateTime && ?dob < "END-01-01T00:00:00Z"^^xsd:dateTime)
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en,mul". }
} ORDER BY ?author ?dob ?pob LIMIT LIMIT_VALUE OFFSET OFFSET_VALUE'''
INSERT = '''INSERT INTO public.wikidata_lb
    (author_code, author_lab, native_name, dob, dod, pob, pob_lab)
    VALUES (%s, %s, %s, %s, %s, %s, %s) ON CONFLICT DO NOTHING'''


def pg_date(value):
    if not value or value.startswith('http'):
        return None
    bc = value.startswith('-')
    value = value.lstrip('+-').split('T')[0]
    if value.startswith('0000-'):
        value = '0001-' + value[5:]
    return value + (' BC' if bc else '')


def convert(row):
    def get(key):
        return row.get(key, {}).get('value')
    return (get('author'), get('authorLabel'), get('nativeName'), pg_date(get('dob')),
            pg_date(get('dod')), get('pob'), get('pobLabel'))


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--start-year', type=positive, default=1)
    parser.add_argument('--end-year', type=positive, default=1900, help='exclusive birth-year boundary')
    parser.add_argument('--limit', type=positive, default=100)
    parser.add_argument('--offset', type=int, default=0)
    parser.add_argument('--batch-size', type=positive, default=25)
    parser.add_argument('--timeout', type=positive, default=60)
    parser.add_argument('--user-agent', help='identifying contact string, required for live requests')
    add_database(parser)
    add_mode(parser)
    args = parser.parse_args(argv)
    if args.start_year >= args.end_year or args.end_year > 9999 or args.offset < 0:
        parser.error('require 1 <= start-year < end-year <= 9999 and offset >= 0')
    query = QUERY.replace('START', f'{args.start_year:04d}').replace('END', f'{args.end_year:04d}').replace('LIMIT_VALUE', str(args.limit)).replace('OFFSET_VALUE', str(args.offset))
    if not (args.write or args.fetch_only):
        print(query)
        return 0
    if not args.user_agent:
        parser.error('--user-agent with contact information is required')
    response = requests.get('https://query.wikidata.org/sparql', params={'query': query, 'format': 'json'},
                            headers={'User-Agent': args.user_agent, 'Accept': 'application/sparql-results+json'}, timeout=args.timeout)
    response.raise_for_status()
    records = [convert(row) for row in response.json()['results']['bindings']]
    if args.write:
        with database(args.env_file, write=True) as conn:
            for offset in range(0, len(records), args.batch_size):
                with conn.transaction(), conn.cursor() as cur:
                    cur.executemany(INSERT, records[offset:offset + args.batch_size])
    print(json.dumps({'records': len(records), 'written': args.write}))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
