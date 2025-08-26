import asyncio
import time
import logging
from typing import (Set,
                    Dict, 
                    Union, 
                    Iterable, 
                    Any)

import aiohttp
from bs4 import BeautifulSoup
import psycopg

from guide2kulchur.privateer.recruits import _rand_headers, _parse_id


def _parse_sim_books_page(txt: str,
                          identifier: str) -> Union[Set[str], str]:
    '''parses similar_books page, returns set of similar_book ID strings
    
    :param txt: similar_books page text
    :param identifer: a book's unique "similar books" ID; this is different from the book's unique ID
    '''
    soup = BeautifulSoup(txt, 'lxml')
    dat = set()    # extremely unlikely to have dupes here, but doesn't hurt
    if list_o_books := soup.find_all('div', class_='responsiveBook'):
        for idx, bk in enumerate(list_o_books):
            if idx == 0:
                continue    # this is the main book itself
            
            if rel_bk_url_tag := bk.find('a', itemprop='url'):
                if rel_bk_url := rel_bk_url_tag.get('href'):
                    abso_bk_url = 'https://www.goodreads.com' + rel_bk_url
                    
                    bk_id = _parse_id(abso_bk_url)
                    dat.add(bk_id)
    
    return dat if len(dat) else identifier


class Envy:
    '''Envy by Yury Olesha'''
    def __init__(self,
                 batch_id: str,
                 cursor: psycopg.Cursor,
                 sim_book_ids: Iterable[str],
                 semaphore_count: int,
                 logger: logging.Logger):
        '''pull similar books'''
        self.batch_id = batch_id
        self.cursor = cursor
        self.sim_book_ids = sim_book_ids
        self.semaphore = asyncio.Semaphore(semaphore_count)
        self.logger = logger

        self.tot_res = []
        self.na = []
        
        self.metadat = {
            'timeouts': 0,
            'timeouts_per_batch_ratio': 0
        }


    async def _get_similar_books(self,
                                 batch_id: int,
                                 session: aiohttp.ClientSession,
                                 semaphore: asyncio.Semaphore,
                                 identifier: str,
                                 num_attempts: int) -> Dict[str,Any]:
        '''pull and return a book's similar books
        FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX
        FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX FIX
        :param session: an aiohttp.ClientSession
        :param semaphore: an asyncio.Semaphore
        :param identifer: a book's unique "similar books" ID; this is different from the book's unique ID
        '''
        sim_books_url = f'https://www.goodreads.com/book/similar/{identifier}'

        async with semaphore:
            attempts = max(1, num_attempts)
            t_start = time.time()

            res = identifier
            for attempt in range(attempts):
                try:
                    async with session.get(url=sim_books_url, 
                                        headers=_rand_headers()) as resp:
                        txt = await resp.text()
                    res = _parse_sim_books_page(txt=txt, identifier=identifier)
                
                except asyncio.TimeoutError:
                    self.metadat['timeouts'] += 1
                    SLEEP_SCALAR = 1.5
                    sleep_time = (attempt + 1) ** SLEEP_SCALAR
                    await asyncio.sleep(sleep_time)
                    self.logger.info('batch %s RETRY sim_id %s', batch_id, identifier)
                
                except Exception as er:
                    self.logger.error('batch %s ERR sim_id %s: %s', batch_id, identifier, er)
                    return res
            
        t_end = time.time()
        t_elapsed = round(t_end - t_start, 3)
        self.logger.info('batch %s T.E. sim_id %s: %s sec.', batch_id, identifier, t_elapsed)

        res_dict = {'sim_id': identifier, 'results': res}
        return res_dict
    

    async def get_sim_books_batch(self,
                                  batch_id: int,
                                  session: aiohttp.ClientSession,
                                  sub_batch_size: int,
                                  sub_batch_delay: int,
                                  num_attempts: int) -> None:
        '''Pull a batch of similar books, format for db insert'''
        tasks = [self._get_similar_books(batch_id=batch_id,
                                        semaphore=self.semaphore,
                                        session=session,
                                        identifier=id_,
                                        num_attempts=num_attempts) for id_ in self.sim_book_ids]
    
        completed = 0
        t_start = time.time()
        async for task in asyncio.as_completed(tasks):
            if completed > 0 and completed % sub_batch_size == 0:
                time.sleep(sub_batch_delay)
            res = await task

            if isinstance(res['results'], set):
                res_4_db = (res['sim_id'], list(res['results']))  # tuple for db insert
                self.tot_res.append(res_4_db)
                
            elif isinstance(res['results'], str):
                self.na.append(res['sim_id'])
                res_4_db = (res['sim_id'], [])  # empty list if not applicable
                self.tot_res.append(res_4_db)

            else:
                self.logger.error('batch %s ERR sim_id %s: %s', batch_id, res['sim_id'], res['results'])
            
            completed += 1
                
        t_end = time.time()
        t_elapsed = round(t_end - t_start, 3)
        self.logger.info('T.E. batch %s: %s sec.', batch_id, t_elapsed)

        success_rate = round(len(self.tot_res) / completed, 3)
        self.logger.info('SUCCESS RATE batch %s: %s', self.batch_id, success_rate)
        self.logger.info('batch %s NA sim_ids: %s', self.batch_id, self.na)

        self.metadat['timeouts_per_batch_ratio'] = round(self.metadat['timeouts'] / completed, 3)
    

    def update_and_insert_db(self):
        '''update sim_books column'''
        create_table_query = '''
                            CREATE TABLE IF NOT EXISTS sim_books
                            (
                                sim_id text PRIMARY KEY,
                                sim_books text[]
                            )
                            '''
        self.cursor.execute(create_table_query)

        truncate_the_table_query = '''TRUNCATE TABLE sim_books'''
        self.cursor.execute(truncate_the_table_query)

        insert_into_sim_books_query = '''
                                        INSERT INTO sim_books 
                                            (sim_id,
                                             sim_books)
                                        VALUES
                                            (%s,
                                             %s)
                                        ON CONFLICT DO NOTHING -- this shouldn't ever happen, but just in case
                                      '''
        self.cursor.executemany(insert_into_sim_books_query, self.tot_res)

        update_and_set_query = '''
                                UPDATE alexandria alx
                                SET sim_books = sb.sim_books
                                FROM sim_books sb
                                WHERE alx.sim_books_url_id = sb.sim_id
                               '''
        self.cursor.execute(update_and_set_query)

