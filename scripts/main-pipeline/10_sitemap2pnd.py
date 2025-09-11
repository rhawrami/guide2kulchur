"""
This script uses the author URLs indexed on the Goodreads author sitemap.
I used the script in 'scripts/supplements/sitmappin_authorIDs.sh' to pull the IDs from there.
The resulting data is in a compressed file 'data/sitemap-dat/final_authorIDs_from_sitemap.txt.gz', which I 
decompressed PRIOR to this script.

Here, we'll load the IDs from that text file (in batches) into memory then store the IDs in a temporary table.
We'll then check if the IDs are already in our db, and if they are, we'll drop them from the table.
Then, like normal, we'll pull the data in batches.

Note that we're gonna get a lot of entries with mostly null results. At the end of this script,
we'll drop rows/authors that don't have at least 1 review and 1 rating.
"""

import asyncio
import os
import time
from typing import Generator

import aiohttp
import psycopg
from dotenv import load_dotenv
load_dotenv()


from guide2kulchur.engineer.batchpullers import BatchAuthorPuller
from guide2kulchur.engineer.recruits import gen_logger, update_sem_and_delay


def pull1ID_fromfile(f_path: str) -> Generator[str]:
    '''yield one ID at a time from a file of author IDs'''
    with open(f_path, 'r') as id_file:
        for id_ in id_file:
            yield id_.strip()


async def main():
    # pull new author, insert into db
    pg_string = os.getenv('PG_STRING')

    # ITER_COUNT * BATCH_SIZE := max number of authors pulled from this script
    ITER_COUNT = 500
    BATCH_SIZE = (500,)

    sem_count = 3   # number of coroutines
    sub_batch_delay = 2   # number of seconds between intra-batch sub-batches
    SUB_BATCH_SIZE = 10   # size of sub-batch
    NUM_ATTEMPTS = 3    # max number of attempts for each pull
    INTER_4BATCH_SLEEP = 10   # number of seconds to sleep on batches divisible by four)

    UPDATE_CFG = {
            'MIN_SEM': 2,
            'MAX_SEM': 10,
            'MIN_DELAY': .2,
            'MAX_DELAY': 5,
            'RATIO_THRESHOLD': .05,
            'DELAY_DELTA': .1
        }

    # logger init
    LOG_DIR = 'sitemap2pound'
    LOG_F = 's2p'
    MAX_B = 1_000_000
    MAX_BACKUPS = 10
    logger = gen_logger(name=LOG_DIR,
                        name_abbr=LOG_F,
                        max_bytes_per_log=MAX_B,
                        max_backups=MAX_BACKUPS)

    timeout = aiohttp.ClientTimeout(total=12,
                                    connect=10)
    connector = aiohttp.TCPConnector(limit=20,
                                     limit_per_host=20,
                                     keepalive_timeout=120,
                                     enable_cleanup_closed=True)

    with psycopg.connect(conninfo=pg_string, autocommit=True) as conn: 
        with conn.cursor() as cur:
            async with aiohttp.ClientSession(timeout=timeout,
                                             connector=connector) as sesh:
                pull_the_ids = True
                if pull_the_ids:    # this way, we can skip this in case of a script error below this point
                    
                    create_temp_id_table = '''
                                            CREATE TABLE IF NOT EXISTS sitemapped_ids (
                                                a_id text PRIMARY KEY   -- speed up ordering (see below)
                                            )
                                        '''
                    cur.execute(create_temp_id_table)

                    SM_BATCH_SIZE = 10000
                    ids2insert = set()  # i don't think there are duplicates, but there may be
                    t_sm_start = time.time()
                    
                    for id_ in pull1ID_fromfile(f_path=os.path.join('data',
                                                                    'sitemap-dat',
                                                                    'final_authorIDs_from_sitemap.txt')):
                        if len(ids2insert) >= SM_BATCH_SIZE:
                            fmt_ids2insert = [(id_,) for id_ in ids2insert]
                            insert_statement = '''
                                                INSERT INTO 
                                                    sitemapped_ids 
                                                        (a_id)
                                                VALUES
                                                    (%s)
                                                ON CONFLICT DO NOTHING
                                            '''
                            cur.executemany(insert_statement, fmt_ids2insert)   # load the IDs into the table
                            ids2insert.clear()  # clear contents

                        if not len(id_) or len(id_) > 8:  # some error lines, just fix here
                            continue
                        else:
                            ids2insert.add(id_)
                    cur.executemany(insert_statement, [(id_,) for id_ in ids2insert])   # remainder batch
                    
                    t_sm_end = time.time()
                    logger.info('SITEMAP2TABLE START QUERY T.E.: %s sec.', round(t_sm_end-t_sm_start, 3))

                    filter_table_2newids = '''
                                            DELETE FROM 
                                                sitemapped_ids
                                            WHERE a_id = ANY(
                                                    SELECT 
                                                        sm.a_id
                                                    FROM
                                                        sitemapped_ids sm
                                                    LEFT JOIN
                                                        pound pnd
                                                    ON sm.a_id = pnd.author_id
                                                    WHERE
                                                        pnd.author_id IS NOT NULL
                                            )
                                        '''
                    cur.execute(filter_table_2newids)

                for batch_id in range(ITER_COUNT):
                    if batch_id > 0 and batch_id % 4 == 0:
                            if batch_id % 10:
                                time.sleep(INTER_4BATCH_SLEEP * 2)  
                            else:
                                time.sleep(INTER_4BATCH_SLEEP)  
                    
                    logger.info('batch %s CFG: SEM-COUNT: %s & SUB-BATCH-DELAY: %s',
                                batch_id, sem_count, sub_batch_delay)
                    starting_point_query_s = time.time()
                    
                    pull_sitemapped_ids = '''
                                          DELETE FROM 
                                            sitemapped_ids
                                          WHERE 
                                            a_id = ANY(array(SELECT a_id FROM sitemapped_ids ORDER BY a_id::int LIMIT %s)) 
                                          RETURNING a_id
                                         '''
                    cur.execute(pull_sitemapped_ids, BATCH_SIZE)

                    starting_point_query_e = time.time()
                    logger.info('batch %s STARTING QUERY: %s sec.', batch_id, round(starting_point_query_e - starting_point_query_s, 3))
                    
                    ids = [r[0] for r in cur.fetchall()]
                    
                    if not len(ids):
                        logger.info('batch %s NO IDs LEFT', batch_id)
                        return None

                    burckhardt = BatchAuthorPuller(batch_id=batch_id,
                                                   cursor=cur, 
                                                   author_ids=ids,
                                                   semaphore_count=sem_count,
                                                   status_logger=logger)
                    try:
                        await burckhardt.load_the_batch(session=sesh,
                                                        num_attempts=NUM_ATTEMPTS,
                                                        see_progress=False,
                                                        batch_delay=sub_batch_delay,
                                                        batch_size=SUB_BATCH_SIZE)
                    except Exception as er:
                         logger.critical('ERR batch %s: %s', batch_id, er)
                         continue
                    
                    try:
                        burckhardt.insert_failed_ids_into_db()  # in case of failed IDs, to ignore in the future
                        burckhardt.insert_batch_into_db()
                    except Exception as er:
                        logger.critical('DB ERR batch %s: %s', batch_id, er)
                        continue
                    
                    # new cfg for next batch
                    sem_count, sub_batch_delay = update_sem_and_delay(current_sem_count=sem_count, 
                                                                      current_sub_batch_delay=sub_batch_delay, 
                                                                      timeouts_per_batch_ratio=burckhardt.metadat['timeouts_per_batch_ratio'],
                                                                      cfg=UPDATE_CFG)
                
                drop_sitemapped_ids_table = '''DROP TABLE IF EXISTS sitemapped_ids'''
                cur.execute(drop_sitemapped_ids_table)


if __name__ == '__main__':
    asyncio.run(main())
