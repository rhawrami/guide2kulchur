"""
This script will be used for MOST future author pulls.
This is just a copy of the process from script #05
"""

import asyncio
import os
import time

import aiohttp
import psycopg
from dotenv import load_dotenv
load_dotenv()


from guide2kulchur.engineer.batchpullers import BatchAuthorPuller
from guide2kulchur.engineer.recruits import gen_logger, update_sem_and_delay


async def main():
    # pull new author, insert into db
    pg_string = os.getenv('PG_STRING')

    # ITER_COUNT * BATCH_SIZE := max number of authors pulled from this script
    ITER_COUNT = 500
    BATCH_SIZE = (300,)

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
    LOG_DIR = 'pnd_ad_infinitum'
    LOG_F = 'pnd'
    MAX_B = 5_000_000
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
                create_pndadinfinitum_table = '''
                                                CREATE TABLE IF NOT EXISTS pnd_ad_infinitum (
                                                    a_id text UNIQUE
                                                )
                                              '''
                cur.execute(create_pndadinfinitum_table)    # we'll need this for the start of our script

                truncate_pndadinfinitum = '''TRUNCATE TABLE pnd_ad_infinitum'''
                cur.execute(truncate_pndadinfinitum)    # clean workspace

                fill_pndadinfinitum = '''
                                        WITH simz (s_id) AS 
                                        (SELECT 
                                            DISTINCT UNNEST(sim_authors) AS s_id 
                                        FROM 
                                            pound 
                                        WHERE 
                                            sim_authors[1] IS NOT NULL)  -- no similar author IDs (don't have to worry about NULLs in the arr)
                                        
                                        INSERT INTO 
                                            pnd_ad_infinitum (a_id)
                                        
                                        SELECT 
                                            simz.s_id
                                        FROM simz 
                                        LEFT JOIN pound 
                                            ON simz.s_id = pound.author_id
                                        WHERE author_id IS NULL
                                        AND NOT EXISTS (SELECT
                                                            1
                                                        FROM error_id
                                                        WHERE
                                                            simz.s_id = item_id 
                                                        AND 
                                                            item_type = 'author' -- so we don't pull author IDs we know aren't valid
                                        )

                                        ON CONFLICT DO NOTHING  -- redundant, but just in case
                                      '''
                start_main_query = time.time()
                cur.execute(fill_pndadinfinitum)
                end_main_query = time.time()
                logger.info('MAIN STARTING QUERY: %s sec.', round(end_main_query - start_main_query, 3))

                for batch_id in range(ITER_COUNT):
                    if batch_id > 0 and batch_id % 4 == 0:
                            if batch_id % 10:
                                time.sleep(INTER_4BATCH_SLEEP * 2)  
                            else:
                                time.sleep(INTER_4BATCH_SLEEP)  
                    
                    logger.info('batch %s CFG: SEM-COUNT: %s & SUB-BATCH-DELAY: %s',
                                batch_id, sem_count, sub_batch_delay)
                    starting_point_query_s = time.time()
                    
                    pull_unentered_ids = '''
                                          DELETE FROM 
                                            pnd_ad_infinitum
                                          WHERE 
                                            a_id = ANY(array(SELECT a_id FROM pnd_ad_infinitum LIMIT %s))
                                          RETURNING a_id
                                         '''
                    cur.execute(pull_unentered_ids, BATCH_SIZE)
                    # note that if a batch fails, you won't pull those unentered IDs until a future batch
                    # this is worth not having to unnest on every batch, which takes way too long

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
                
                drop_pndadinfinitum = '''DROP TABLE IF EXISTS pnd_ad_infinitum'''
                cur.execute(drop_pndadinfinitum)


if __name__ == '__main__':
    asyncio.run(main())
