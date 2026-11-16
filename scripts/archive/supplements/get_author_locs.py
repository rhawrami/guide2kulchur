'''
This script uses Nominatim to find the address and coordinates
for an author's listed birth place. Nominatim's free API allows for
searching the address, latitude, and longitude of some location string. 
While it's not perfect, it's relatively fast, free and reliable, and I appreciate
the service greatly.

Nominatim Link Here: https://nominatim.org/
Geopy Link Here: https://geopy.readthedocs.io/en/stable/

Given that there are a lot of repeated birth_place values, at the start I grouped the values to 
get distinct strings, and limit to those strings with at least 5 authors belonging to
them.
UPDATE: I am now trying all distinct birth_place values, including those where there's only one author
with the value. This is going to be very messy, but worth it for the broader range of locations.

This script batches database inserts after 100 pulls, which should take about ~200 seconds. With
the way that the location strings are loaded in, if the script fails halfway through, you can restart
the script and essentially pick up where you left off.
'''

import asyncio
import re
import os
import json
import time
import logging
from typing import Dict, Any

import psycopg
from psycopg.rows import dict_row
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
from geopy.adapters import AioHTTPAdapter
from geopy.extra.rate_limiter import AsyncRateLimiter
from dotenv import load_dotenv
load_dotenv()


# iso 3166-1 alpha-2 codes
# read more on it here: https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2
# json file taken from here: https://github.com/fannarsh/country-list
ISO_PATH = os.path.join('data', 'iso', 'iso_codes.json')
with open(ISO_PATH, 'r') as ip:
    ISO_COUNTRY_CODE_MAP = json.load(ip)
ISO_COUNTRY_CODE_MAP = {ic['name'].lower(): ic['code'].lower() for ic in ISO_COUNTRY_CODE_MAP}


# for some of the larger countries (in share of GR authors), it's worth manually parsing
# them. We'll use the dictionary above as last resort.
COUNTRY_CFG = {
    'us': {
        'match_pat': r'(,? the)? united states( of america)?',
        'country_code': 'us',
        'replace_with': ''
    },
    'de': {
        'match_pat': r', .*german.*$',
        'country_code': 'de',
        'replace_with': ', germany'
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
    'kr': {
        'match_pat': r'(, )?(south|republic of) korea.*$',
        'country_code': 'kr',
        'replace_with': '' 
    },
    'va': {
        'match_pat': r'^.*holy see.*vatican.*$',
        'country_code': 'va',
        'replace_with': 'vatican city'
    },
    'in': {
        'match_pat': r'(, )?(british )?india.*$',
        'country_code': 'in',
        'replace_with': ', india'
    }
}


def new_logger(file_name: str) -> logging.Logger:
    # get basic logger
    logger = logging.getLogger("birth_place")
    logger.setLevel(logging.DEBUG)
    # file handler
    fh = logging.FileHandler(filename=file_name)
    fh.setLevel(logging.INFO)
    fh_fmt = logging.Formatter(fmt='%(asctime)s %(levelname)s - %(message)s', 
                               datefmt='%m-%d-%Y %H:%M:%S')
    fh.setFormatter(fh_fmt)
    # add
    logger.addHandler(fh)

    return logger


async def _send_req(fn: Nominatim.geocode,
                    geocode_kwargs: Dict[str, Any]) -> Dict[str, Any]:
    # actually send the request
    data = {
        'res': None,
        'err': None
    }
    
    try:
        res = await fn(**geocode_kwargs)
        data['res'] = res
    except GeocoderTimedOut as err:
        data['err'] = err
    except Exception as err:
        data['err'] = err
    
    return data
    

async def send_loc_req(loc_str: str, 
                       fn: Nominatim.geocode,
                       logger: logging.Logger) -> Dict[str, Any]:
    
    # clean location string, send geocode request
    dat = {
        'og_loc': loc_str,
        'submitted_string': loc_str,
        'country_code': None,
        'addr': None,
        'lat': None,
        'lon': None
    }
    geocode_kwargs = {'query': loc_str}

    # delete all parantheses from strings
    dat['submitted_string'] = re.sub(r'\([\w\s]+\)', '', dat['submitted_string'])

    # some strings have XXXX, XXXX, republic of
    # this messes up searches; let's fix it
    rep_of_match_pat = r'(.*,) (.*,) (.*republic of ?t?h?e?)$'
    if (rep_match := re.match(rep_of_match_pat, dat['submitted_string'])):
        # flip group 2 and 3
        subbed_str = f'{rep_match.group(1).replace(r',', '')}, {rep_match.group(3)} {rep_match.group(2).replace(r',', '')}'
        dat['submitted_string'] = subbed_str
        geocode_kwargs['query'] = subbed_str
    
    # some strings have dates in them, due to parsing errors earlier on
    date_match_pat = r'\d{2,4}[\/-]\d{2}[\/-]\d{2,4}|\d{4}'
    if re.search(date_match_pat, dat['submitted_string']):
        subbed_str = re.sub(date_match_pat, '', dat['submitted_string'])
        dat['submitted_string'] = subbed_str
        geocode_kwargs['query'] = subbed_str

    # check manual configuration first
    for country, cfg in COUNTRY_CFG.items():
        if re.search(cfg['match_pat'], dat['submitted_string']):
            subbed_str = re.sub(cfg['match_pat'], cfg['replace_with'], dat['submitted_string'])
            dat['submitted_string'] = subbed_str
            geocode_kwargs['query'] = subbed_str
            geocode_kwargs['country_codes'] = cfg['country_code']
            dat['country_code'] = cfg['country_code']
            break

    # now use the iso dictionary as last resort
    if not dat['country_code']:
        comma_pat = r'^(.*, )?(.*), (.*)$'
        if comma_pat_match := re.match(comma_pat, dat['submitted_string']):
            for name, code in ISO_COUNTRY_CODE_MAP.items():
                if comma_pat_match.group(3) in name:
                    # select 2nd group, assuming its broader, e.g., more likely to get a match
                    subbed_str = comma_pat_match.group(2)
                    dat['submitted_string'] = subbed_str
                    geocode_kwargs['query'] = subbed_str
                    geocode_kwargs['country_codes'] = code
                    dat['country_code'] = code
                    break

    resp = await _send_req(fn=fn, 
                           geocode_kwargs=geocode_kwargs)
    if resp['err']:
        logger.error('ERR FOR %s: %s', dat['submitted_string'], dat['err'])
        return dat
    
    if resp['res']:
        dat['addr'] = resp['res'].address
        dat['lat'] = resp['res'].latitude
        dat['lon'] = resp['res'].longitude

    return dat

    
async def main():
    # logger
    LOG_F_NAME = "get_birth_locs.log"
    logger = new_logger(LOG_F_NAME)
    # conn string
    PG_STRING: str = os.getenv("PG_STRING")
    # location group size cutoff
    LOC_GRP_SIZE_CUTOFF: int = (1,)
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
                    send_loc_req(loc_str=loc['bp'], fn=geocode_fn, logger=logger)
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
                n_successes, n_fails = 0, 0
                time_start_batch = time.time()
                async for task in asyncio.as_completed(tasks):
                    res = await task
                    resTuple = (res['og_loc'], res['addr'], res['lat'], res['lon'])
                    if not res['addr']:
                        # log fails
                        logger.info('FAIL ON %s FOR SUBSTR:%s AND CC: %s', 
                                    f'{on_task}/{tot_tasks}', res['submitted_string'], res['country_code'])
                        n_fails += 1
                    else:
                        n_successes += 1
                    
                    # insert batch
                    if len(batch) >= MAX_BATCH_SIZE:
                        time_end_batch = time.time()
                        pull_rate = MAX_BATCH_SIZE / (time_end_batch - time_start_batch)
                        logger.info('BATCH PULL RATE OF %.2f PULLS/SEC', pull_rate)
                        cur.executemany(insert_statement, batch)
                        batch.clear()
                        time_start_batch = time.time() # reset timer
                    batch.append(resTuple)
                    
                    # every ten pulls, print progress
                    if (on_task % 10 == 0) or (on_task == tot_tasks):
                        logger.info('PROGRESS %s', f'{on_task}/{tot_tasks}')
                        logger.info('CUM. SUCCESS RATE OF %.2f', (n_successes / (n_successes + n_fails)))
                    on_task += 1

                # final batch
                cur.executemany(insert_statement, batch)
        

if __name__ == '__main__':
    asyncio.run(main())
    

