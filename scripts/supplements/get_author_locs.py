'''
This script uses Nominatim to find the address and coordinates
for an author's listed birth place. Nominatim's free API allows for
searching the address, latitude, and longitude of some location string. 
While it's not perfect, it's relatively fast, free and reliable, and I appreciate
the service greatly.

Nominatim Link Here: https://nominatim.org/
Geopy Link Here: https://geopy.readthedocs.io/en/stable/

Given that there are a lot of repeated birth_place values, I group the values to 
get distinct strings, and limit to those strings with at least 3 authors belonging to
them. Further, I add 'US' flags to strings that are likely to be in the US. 

This script batches database inserts after 100 pulls, which should take about ~200 seconds. With
the way that the location strings are loaded in, if the script fails halfway through, you can restart
the script and essentially pick up where you left off.
'''

import asyncio
import re
import os
from typing import Dict, Any

import psycopg
from psycopg.rows import dict_row
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
from geopy.adapters import AioHTTPAdapter
from geopy.extra.rate_limiter import AsyncRateLimiter
from dotenv import load_dotenv
load_dotenv()


COUNTRY_CFG = {
    'us': {
        'match_pat': r'(,? the)? united states( of america)?',
        'country_code': 'us',
        'replace_with': ', USA'
    },
    'gb': {
        'match_pat': r'(,? the)? united kingdom| england',
        'country_code': 'gb',
        'replace_with': ''
    },
    'ru': {
        'match_pat': r', (ussr|russian?).*$',
        'country_code': 'ru',
        'replace_with': ''
    },
    'ir': {
        'match_pat': r',.*(iran|persia).*$',
        'country_code': 'ir',
        'replace_with': ''
    },
    'ps': {
        'match_pat': r'palestinian territory, occupied',
        'country_code': 'ps',
        'replace_with': 'palestine'
    },
    'tr': {
        'match_pat': r'(, ottoman empire,) turkey',
        'country_code': 'tr',
        'replace_with': ''
    },
    'dprk': {
        'match_pat': r'^.*korea.*democratic people.s republic of.*$',
        'country_code': 'kp',
        'replace_with': 'pyongyang' # gotta pick something for this to work
    },
    'va': {
        'match_pat': r'^.*holy see.*vatican.*$',
        'country_code': 'va',
        'replace_with': 'vatican city'
    }
}


async def send_loc_req(loc_str: str, 
                       fn: Nominatim.geocode) -> Dict[str, Any]:
    dat = {
        'og_loc': loc_str,
        'submitted_string': loc_str,
        'addr': None,
        'lat': None,
        'lon': None
    }
    geocode_kwargs = {'query': loc_str}

    for country, cfg in COUNTRY_CFG.items():
        if re.search(cfg['match_pat'], loc_str):
            subbed_str = re.sub(cfg['match_pat'], cfg['replace_with'], loc_str)
            dat['submitted_string'] = subbed_str
            geocode_kwargs['query'] = subbed_str
            geocode_kwargs['country_codes'] = cfg['country_code']
            break
    
    # some strings have XXXX, XXXX, republic of
    # this messes up searches; let's fix it
    rep_of_match_pat = r'(.*,) (.*,) (.* republic of ?t?h?e?)$'
    if (rep_match := re.match(rep_of_match_pat, loc_str)):
        # flip group 2 and 3
        subbed_str = f'{rep_match.group(1).replace(r',', '')}, {rep_match.group(3)} {rep_match.group(2).replace(r',', '')}'
        dat['submitted_string'] = subbed_str
        geocode_kwargs['query'] = subbed_str

    try:
        res = await fn(**geocode_kwargs)
    except GeocoderTimedOut:
        return dat
    
    if res:
        dat['addr'] = res.address
        dat['lat'] = res.latitude
        dat['lon'] = res.longitude
    return dat

    
async def main():
    # conn string
    PG_STRING: str = os.getenv("PG_STRING")
    # location group size cutoff
    LOC_GRP_SIZE_CUTOFF: int = (3,)
    # query to get location strings
    loc_query: str =    '''
                        WITH 
                            raw_locs(bp)
                        AS (
                            SELECT 
                                lower(birth_place) AS bp
                            FROM 
                                pound
                            WHERE 
                                birth_place IS NOT NULL
                            AND
                                lower(birth_place) <> 'born' -- parsing issues, so some authors have 'Born' as their birth_place
                            GROUP BY
                                lower(birth_place)
                            HAVING 
                                COUNT(birth_place) >= %s
                        )
                        SELECT 
                            bp
                        FROM
                            raw_locs
                        LEFT JOIN
                            birth_place_locs
                        ON
                            bp = og_loc
                        WHERE 
                            og_loc IS NULL -- e.g., not loaded into the final location table yet
                        '''

    with psycopg.connect(conninfo=PG_STRING, 
                         autocommit=True,
                         row_factory=dict_row) as conn:
        with conn.cursor() as cur:
            # for repeat runs, assuming you change some criteria
            # so that you'll get a match
            cleanup_statement = '''
                                DELETE FROM 
                                    birth_place_locs
                                WHERE 
                                    addr IS NULL
                                '''
            cur.execute(cleanup_statement)

            # get location strings
            cur.execute(loc_query, LOC_GRP_SIZE_CUTOFF)
            # async geolocator
            # cfg
            SEC_DELAY_BETWEEN_REQ = 1
            SEC_TIMEOUT_AFTER = 10
            # async geocoder
            async with Nominatim(
                timeout=SEC_TIMEOUT_AFTER,
                user_agent='guide2kulchur',
                adapter_factory=AioHTTPAdapter
            ) as locator:
                # built in rate limit
                geocode_fn = AsyncRateLimiter(locator.geocode, min_delay_seconds=SEC_DELAY_BETWEEN_REQ)
                tasks = [
                    send_loc_req(loc_str=loc['bp'], fn=geocode_fn)
                    for loc
                    in cur.fetchall()
                ]

                MAX_BATCH_SIZE = 100
                batch = []
                tot_tasks = len(tasks)
                on_task = 1
                insert_statement = '''
                                    INSERT INTO 
                                        birth_place_locs(og_loc, addr, lat, lon)
                                    VALUES
                                        (%s,%s,%s,%s)
                                   '''
                async for task in asyncio.as_completed(tasks):
                    res = await task
                    resTuple = (res['og_loc'], res['addr'], res['lat'], res['lon'])
                    if not res['addr']:
                        # need to think about what to do with fails
                        print(f'FAIL ({on_task}/{tot_tasks}): {res['og_loc']}')

                    if len(batch) >= MAX_BATCH_SIZE:
                        cur.executemany(insert_statement, batch)
                        batch.clear()
                    batch.append(resTuple)
                    
                    # every ten pulls, print progress
                    if (on_task % 10 == 0) or (on_task == tot_tasks):
                        print(f'{on_task}/{tot_tasks} completed')
                    on_task += 1

                # final batch
                cur.executemany(insert_statement, batch)
        

if __name__ == '__main__':
    asyncio.run(main())
    

