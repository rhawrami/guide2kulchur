"""
This script pulls distinct book_id entries from the book_sample attr from pound, 
then pulls data on those books,
then stores results into alexandria.
"""

import asyncio
import os
import time

import aiohttp
import psycopg
from dotenv import load_dotenv
load_dotenv()


from guide2kulchur.engineer.batchpullers import BatchBookPuller
from guide2kulchur.engineer.recruits import gen_logger, update_sem_and_delay


async def main():
    # pull new books, insert into db
    pg_string = os.getenv('PG_STRING')

    # ITER_COUNT * BATCH_SIZE := max number of books pulled from this script
    ITER_COUNT = 500
    BATCH_SIZE = (300,)

    # RATING THRESHOLD
    # This is important: if we don't have a cutoff, 
        # we'd essentially pull (<=8 * len(pound.author_id) -  <=len(alexandria.book_id)) IDs
        # many of which would have no ratings themselves
    # Thus, this threshold on an author's rating count will act as a filter for the 
    # books we're going to pull.
    RATING_THRESHOLD = (500,)  # start at 500

    sem_count = 5   # number of coroutines
    sub_batch_delay = 1.5   # number of seconds between intra-batch sub-batches
    SUB_BATCH_SIZE = 10   # size of sub-batch
    NUM_ATTEMPTS = 3    # max number of attempts for each pull
    INTER_4BATCH_SLEEP = 10   # number of seconds to sleep on batches divisible by four)

    UPDATE_CFG = {
            'MIN_SEM': 2,
            'MAX_SEM': 18,
            'MIN_DELAY': .2,
            'MAX_DELAY': 5,
            'RATIO_THRESHOLD': .05,
            'DELAY_DELTA': .1
        }

    # logger init
    LOG_DIR = 'pnd2alx'
    LOG_F = 'pnd2alx'
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
                create_pnd2alx_table = '''
                                        CREATE TABLE IF NOT EXISTS pnd2alx (
                                            b_id text PRIMARY KEY
                                        )
                                       '''
                cur.execute(create_pnd2alx_table)    # use this as an ID queue

                truncate_pnd2alx = '''TRUNCATE TABLE pnd2alx'''
                cur.execute(truncate_pnd2alx)    # clean workspace

                fill_pnd2alx = '''
                                WITH pounder (b_id) AS 
                                (SELECT 
                                    DISTINCT UNNEST(book_sample) AS b_id 
                                FROM 
                                    pound 
                                WHERE 
                                    rating_count >= %s)  
                                
                                INSERT INTO 
                                    pnd2alx (b_id)
                                
                                SELECT 
                                    pounder.b_id
                                FROM pounder 
                                LEFT JOIN alexandria 
                                    ON pounder.b_id = alexandria.book_id
                                WHERE alexandria.book_id IS NULL
                                AND NOT EXISTS (SELECT
                                                    1
                                                FROM error_id
                                                WHERE
                                                    pounder.b_id = item_id 
                                                AND 
                                                    item_type = 'book' -- so we don't pull book IDs we know aren't valid
                                )

                                ON CONFLICT DO NOTHING  -- redundant, but just in case
                                '''
                start_main_query = time.time()
                cur.execute(fill_pnd2alx, RATING_THRESHOLD)
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
                                            pnd2alx
                                          WHERE 
                                            b_id = ANY(array(SELECT b_id FROM pnd2alx LIMIT %s))
                                          RETURNING b_id
                                         '''
                    cur.execute(pull_unentered_ids, BATCH_SIZE)
                    # note that if a batch fails, you won't pull those unentered IDs until a future batch

                    starting_point_query_e = time.time()
                    logger.info('batch %s STARTING QUERY: %s sec.', batch_id, round(starting_point_query_e - starting_point_query_s, 3))
                    
                    ids = [r[0] for r in cur.fetchall()]
                    
                    if not len(ids):
                        logger.info('batch %s NO IDs LEFT', batch_id)
                        return None

                    marble_cliffs = BatchBookPuller(batch_id=batch_id,
                                                    cursor=cur, 
                                                    book_ids=ids,
                                                    semaphore_count=sem_count,
                                                    status_logger=logger)
                    try:
                        await marble_cliffs.load_the_batch(session=sesh,
                                                            num_attempts=NUM_ATTEMPTS,
                                                            see_progress=False,
                                                            batch_delay=sub_batch_delay,
                                                            batch_size=SUB_BATCH_SIZE)
                    except Exception as er:
                         logger.critical('ERR batch %s: %s', batch_id, er)
                         continue
                    
                    try:
                        marble_cliffs.insert_failed_ids_into_db()  # in case of failed IDs, to ignore in the future
                        marble_cliffs.insert_batch_into_db()
                    except Exception as er:
                        logger.critical('DB ERR batch %s: %s', batch_id, er)
                        continue
                    
                    # new cfg for next batch
                    sem_count, sub_batch_delay = update_sem_and_delay(current_sem_count=sem_count, 
                                                                      current_sub_batch_delay=sub_batch_delay, 
                                                                      timeouts_per_batch_ratio=marble_cliffs.metadat['timeouts_per_batch_ratio'],
                                                                      cfg=UPDATE_CFG)
                
                drop_pnd2alx = '''DROP TABLE IF EXISTS pnd2alx'''
                cur.execute(drop_pnd2alx)


if __name__ == '__main__':
    asyncio.run(main())
