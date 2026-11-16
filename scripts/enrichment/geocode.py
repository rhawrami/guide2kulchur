"""Geocode missing author birthplaces; preview by default."""
import argparse
import asyncio
import json

from geopy.adapters import AioHTTPAdapter
from geopy.extra.rate_limiter import AsyncRateLimiter
from geopy.geocoders import Nominatim

from guide2kulchur.pipeline.common import add_database, add_mode, database, nonnegative, positive

QUERY = '''SELECT lower(p.birth_place) AS location FROM public.pound p
    WHERE p.birth_place IS NOT NULL AND lower(trim(p.birth_place)) NOT IN ('', 'born')
    AND NOT EXISTS (SELECT 1 FROM public.birth_place_locs l
                    WHERE l.og_loc = lower(p.birth_place) AND l.addr IS NOT NULL)
    GROUP BY lower(p.birth_place) HAVING count(*) >= %s
    ORDER BY count(*) DESC, lower(p.birth_place) LIMIT %s'''
INSERT = '''INSERT INTO public.birth_place_locs (og_loc, addr, lat, lon) VALUES (%s, %s, %s, %s)
    ON CONFLICT (og_loc) DO UPDATE SET addr = EXCLUDED.addr, lat = EXCLUDED.lat, lon = EXCLUDED.lon
    WHERE birth_place_locs.addr IS NULL'''


async def geocode(conn, locations, args):
    async with Nominatim(user_agent=args.user_agent, timeout=args.timeout, adapter_factory=AioHTTPAdapter) as locator:
        # Single serial caller, at least one second apart; no error-swallowing retry storm.
        request = AsyncRateLimiter(locator.geocode, min_delay_seconds=args.request_delay,
                                   max_retries=0, swallow_exceptions=False)
        batch = []
        for location in locations:
            result = await request(location)
            if result:
                batch.append((location, result.address, result.latitude, result.longitude))
            print(json.dumps({'location': location, 'found': result is not None}))
            if len(batch) >= args.batch_size:
                if args.write:
                    with conn.transaction(), conn.cursor() as cur:
                        cur.executemany(INSERT, batch)
                batch.clear()
        if batch and args.write:
            with conn.transaction(), conn.cursor() as cur:
                cur.executemany(INSERT, batch)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--limit', type=positive, default=100)
    parser.add_argument('--batch-size', type=positive, default=25)
    parser.add_argument('--min-authors', type=positive, default=1)
    parser.add_argument('--request-delay', type=nonnegative, default=1)
    parser.add_argument('--timeout', type=positive, default=15)
    parser.add_argument('--user-agent', help='identifying application/contact string, required for live requests')
    add_database(parser)
    add_mode(parser)
    args = parser.parse_args(argv)
    if args.request_delay < 1:
        parser.error('Nominatim requires --request-delay >= 1')
    if (args.write or args.fetch_only) and not args.user_agent:
        parser.error('--user-agent with identifying contact information is required')
    with database(args.env_file, write=args.write) as conn:
        locations = [row[0] for row in conn.execute(QUERY, (args.min_authors, args.limit))]
        if not (args.write or args.fetch_only):
            print(json.dumps({'mode': 'preview', 'locations': locations}, indent=2))
        else:
            asyncio.run(geocode(conn, locations, args))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
