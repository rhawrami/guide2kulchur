"""
Now that we have our initial set of books (~60k), we're going to pull each book's 
set of "similar books," as defined by Goodreads. With this attribute added to our 
database, we'll be able to continue pulling more books in an almost recursive fashion.
"""

import asyncio
import os
import time

import aiohttp
import psycopg
from dotenv import load_dotenv
load_dotenv()

from guide2kulchur.engineer.envy import Envy
from guide2kulchur.engineer.recruits import gen_logger, update_sem_and_delay


async def main():
    # update sim_books in alexandria
    NAME = 'sim_books_first'
    NAME_ABBR = 'sb_f'
    max_b = 20000
    max_backups = 5

    logger = gen_logger(name=NAME, 
                        name_abbr=NAME_ABBR,
                        max_bytes_per_log=max_b,
                        max_backups=max_backups)
    
    pg_string = os.getenv('PG_STRING')

    # config timeout and connector
    timeout = aiohttp.ClientTimeout(total=15,
                                    connect=10)
    connector = aiohttp.TCPConnector(limit=20,
                                     limit_per_host=20,
                                     keepalive_timeout=120,
                                     enable_cleanup_closed=True)
    
    with psycopg.connect(conninfo=pg_string,
                         autocommit=True) as conn:
        with conn.cursor() as cur:
            async with aiohttp.ClientSession(timeout=timeout,
                                             connector=connector) as sesh:
                
                sem_count = 3   
                sub_batch_delay = 2  
                SUB_BATCH_SIZE = 10  
                NUM_ATTEMPTS = 3    
                INTER_3BATCH_SLEEP = 10  

                NUM_ITER = 2000  # I'll run this batch processing 2000 times (e.g., 20k books pulled), and just rerun the script until all obs are updated

                for batch_id in range(NUM_ITER):
                    if batch_id % 3 == 0:
                        time.sleep(INTER_3BATCH_SLEEP)  

                    logger.info('batch %s CFG: SEM-COUNT: %s & SUB-BATCH-DELAY: %s',
                                batch_id, sem_count, sub_batch_delay)

                    fetch_some_sim_ids_query = '''
                                                SELECT sim_books_url_id
                                                FROM alexandria
                                                WHERE sim_books IS NULL
                                                LIMIT 100                   -- batch size == 100
                                               '''
                    cur.execute(fetch_some_sim_ids_query)
                    sim_ids = {r[0] for r in cur.fetchall()}
                    
                    if not len(sim_ids):
                        logger.info('batch %s NO TUPLES LEFT', batch_id)
                        break

                    olesha = Envy(batch_id=batch_id, 
                                  cursor=cur,
                                  sim_book_ids=sim_ids,
                                  semaphore_count=sem_count,
                                  logger=logger)
                    
                    try:
                        await olesha.get_sim_books_batch(batch_id=batch_id,
                                                         session=sesh,
                                                         sub_batch_size=SUB_BATCH_SIZE,
                                                         sub_batch_delay=sub_batch_delay,
                                                         num_attempts=NUM_ATTEMPTS)
                    except Exception as er:
                        logger.critical('ERR batch %s: %s', batch_id, er)   # not sure what would trigger this, but just in case
                        continue
                    
                    try:
                        olesha.update_and_insert_db()
                    except Exception as er:
                        logger.critical('DB ERR batch %s: %s', batch_id, er)
                        continue
                    
                    CFG = {
                        'MIN_SEM': 3, 'MAX_SEM': 6,
                        'MIN_DELAY': .25, 'MAX_DELAY': 5,
                        'RATIO_THRESHOLD': .1, 'DELAY_DELTA': .5
                    }
                    sem_count, sub_batch_delay = update_sem_and_delay(current_sem_count=sem_count,
                                                                      current_sub_batch_delay=sub_batch_delay,
                                                                      timeouts_per_batch_ratio=olesha.metadat['timeouts_per_batch_ratio'],
                                                                      cfg=CFG)
            
            rm_sim_books_table_query = '''DROP TABLE IF EXISTS sim_books'''
            cur.execute(rm_sim_books_table_query)
                    
                    


if __name__ == '__main__':
    asyncio.run(main())

