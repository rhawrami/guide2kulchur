"""
This script will be the beginning of our data collection. Here, we'll use the book IDs we already
pulled in the past two scripts, and collect data on each book. We'll then store the data in a database
using Postgres. The process will be as follows:

1. load up top-shelved-books data, and aggregate the book IDs; filter out duplicates.
2. transform the aggregate ID set into batches.
3. collect data in batches, with dynamic delaying and semaphoring.
4. load the data into our database.

Throughout this process, we'll also log all our progress and errors we encounter along the way.
"""

import asyncio
import os
import time
import sys
import json
import logging
from logging.handlers import RotatingFileHandler
from typing import (
    Tuple,
    Set
)

import aiohttp
import psycopg
from dotenv import load_dotenv
load_dotenv()

from guide2kulchur.engineer.houseofwisdom import BatchBookPuller


def gen_logger() -> logging.Logger:
    '''set up logger object'''
    logger = logging.getLogger('top_shelved_books')
    logger.setLevel(logging.DEBUG)

    # make dirs
    os.makedirs('logs', exist_ok=True)
    os.makedirs(os.path.join('logs','top_shelved'), exist_ok=True)

    # progress statements; e.g., time-to-complete batch #5
    PROG_PATH = os.path.join('logs', 'top_shelved', 'ts_prog.log')
    prog_handler = RotatingFileHandler(filename=PROG_PATH,
                                       maxBytes=50000,
                                       backupCount=10)
    prog_handler.setLevel(logging.INFO)
    prog_fmt = logging.Formatter(fmt='%(asctime)s %(levelname)s := %(message)s',
                                 datefmt='%m-%d-%Y %H:%M:%S')
    prog_handler.setFormatter(prog_fmt)

    # stream to stdout
    stream_handler = logging.StreamHandler(stream=sys.stdout)
    stream_handler.setLevel(logging.INFO)
    stream_fmt = logging.Formatter(fmt='%(asctime)s %(levelname)s := %(message)s',
                                   datefmt='%m-%d-%Y %H:%M:%S')
    stream_handler.setFormatter(stream_fmt)

    # error statements; e.g., book ID 7777777 failed
    ERR_PATH = os.path.join('logs', 'top_shelved', 'ts_err.log')
    err_handler = RotatingFileHandler(filename=ERR_PATH,
                                      maxBytes=20000,
                                      backupCount=10)
    err_handler.setLevel(logging.ERROR)
    err_fmt = logging.Formatter(fmt='%(asctime)s %(levelname)s := %(message)s',
                                datefmt='%m-%d-%Y %H:%M:%S')
    err_handler.setFormatter(err_fmt)

    logger.addHandler(prog_handler)
    logger.addHandler(err_handler)
    logger.addHandler(stream_handler)
    return logger


def get_top_shelved_ids(path: str) -> Set[str]:
    '''returns set of Goodreads top-shelved book IDs'''
    with open(path,'r') as ts_f:
        ts = json.load(ts_f)

    uniq_ids = set()    # this will remove all duplicate IDs
    for id_block in ts['results'].values():
        uniq_ids.update(id_block)
    return list(uniq_ids)
    

def update_sem_and_delay(current_sem_count: int,
                         current_sub_batch_delay: int,
                         timeouts_per_batch_ratio: float) -> Tuple[int,int]:
    '''takes in batch metadata, returns new SEMAPHORE COUNT & SUB_BATCH_DELAY'''
    MIN_SEM, MAX_SEM = 2, 9
    MIN_DELAY, MAX_DELAY = .25, 5

    DELAY_DELTA = .1

    scalar = -1 if timeouts_per_batch_ratio > 0.05 else 1
    new_sem_count = (lambda x: sorted([MIN_SEM, current_sem_count + x, MAX_SEM])[1])(scalar)
    new_delay_count = (lambda x: sorted([MIN_DELAY, current_sub_batch_delay - DELAY_DELTA * x, MAX_DELAY])[1])(scalar)
    
    return new_sem_count, new_delay_count


async def main():
    # get top-shelved ids
    TS_IDS_PATH = os.path.join('data','genres','top_shelved_ids.json')
    ts_ids = get_top_shelved_ids(TS_IDS_PATH)
    batches = [ts_ids[i:i+100] for i in range(0, len(ts_ids), 100)]   # batch size of 50
    # get logger
    os.makedirs('logs', exist_ok=True)
    logger = gen_logger()

    # pg conn string
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

                # some config
                sem_count = 3   # variable, will change based on success rate
                sub_batch_delay = 2   # variabe, will change based on success rate
                SUB_BATCH_SIZE = 10   # hard coded at 10
                NUM_ATTEMPTS = 3    # hard-code at 3
                INTER_3BATCH_SLEEP = 10   # hard-coded at 10
                
                for batch_id,batch in enumerate(batches):
                    if batch_id % 3 == 0:
                        time.sleep(INTER_3BATCH_SLEEP)  # every three batches (ignore first round), sleep for 10 seconds

                    logger.info('batch %s CFG: SEM-COUNT: %s & SUB-BATCH-DELAY: %s',
                                batch_id, sem_count, sub_batch_delay)
                    # init batch puller
                    batch_pull = BatchBookPuller(batch_id=batch_id,
                                                 cursor=cur,
                                                 book_ids=batch,
                                                 semaphore_count=sem_count,
                                                 status_logger=logger)
                    
                    # pull data on current batch's book IDs
                    try: 
                        await batch_pull.load_the_batch(session=sesh,
                                                        num_attempts=NUM_ATTEMPTS,
                                                        see_progress=False, # streaming logs to stdout anyway, no need
                                                        batch_delay=sub_batch_delay,
                                                        batch_size=SUB_BATCH_SIZE)
                    except Exception as er:
                        logger.critical('ERR batch %s: %s', batch_id, er)
                        continue
                    
                    # insert data into the db
                    try:
                        batch_pull.insert_batch_into_db()
                    except Exception as er:
                        logger.critical('DB ERR batch %s: %s', batch_id, er)
                        continue
                    
                    # new cfg for next batch
                    sem_count, sub_batch_delay = update_sem_and_delay(sem_count, 
                                                                      sub_batch_delay, 
                                                                      batch_pull.metadat['timeouts_per_batch_ratio'])
            

if __name__ == '__main__':
    asyncio.run(main())
