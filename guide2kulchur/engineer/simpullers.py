import asyncio
import time
import logging
from abc import ABC, abstractmethod
from typing import (Optional, 
                    Dict, 
                    Union, 
                    Iterable, 
                    Any,
                    Set)

import aiohttp
import psycopg
from bs4 import BeautifulSoup

from guide2kulchur.privateer.recruits import _rand_headers, _parse_id


def _parse_sim_books_page(txt: str) -> Union[Set[str], str]:
    '''parses similar_books page, returns set of similar_book ID strings
    
    :param txt: similar_books page text
    '''
    soup = BeautifulSoup(txt, 'lxml')
    if head1 := soup.find('h1').text.lower().strip() != 'readers who enjoyed':
        return []     # no sim books page, redirected to main page
    
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
    
    return list(dat)


def _parse_sim_authors_page(txt: str) -> Union[Set[str], str]:
    '''parses similar_authors page, returns set of similar_author ID strings
    
    :param txt: similar_authors page text
    '''
    soup = BeautifulSoup(txt, 'lxml')
    if head1 := soup.find('h1').text.lower().strip() != 'members who read books by':
        return []
    dat = set()    # extremely unlikely to have dupes here, but doesn't hurt
    if list_o_authors := soup.find_all('div', class_='responsiveAuthor'):
        for idx, bk in enumerate(list_o_authors):
            if idx == 0:
                continue    # this is the main author themselves
            
            if rel_athr_url_tag := bk.find('a', itemprop='url'):
                if rel_athr_url := rel_athr_url_tag.get('href'):
                    abso_athr_url = 'https://www.goodreads.com' + rel_athr_url
                    
                    athr_id = _parse_id(abso_athr_url)
                    dat.add(athr_id)
    
    return list(dat)


class SimItemsPuller(ABC):
    '''Pull a batch of similar items (books | authors)'''
    def __init__(self,
                 batch_id: Union[int,str],
                 cursor: psycopg.Cursor,
                 sim_item_type: str,
                 sim_item_ids: Iterable[str],
                 semaphore_count: int,
                 status_logger: logging.Logger):
          '''pull Goodreads item data.
          
          :batch_id: batch identifier; used for logging
          :param cursor: a psycopg Cursor object
          :param item_type: Goodreads item data type (book|author|user) 
          :param sim_item_ids: an iterable of Goodreads similar item (book|author) IDs
          :param semaphore_counr: number of maximum concurrent coroutines
          :param status_logger: a Logger object to record progress/status/issues
          '''
          self.batch_id = batch_id
          self.cursor = cursor
          self.sim_item_type = sim_item_type
          self.sim_item_ids = sim_item_ids
          self.semaphore_count = semaphore_count
          self.semaphore = asyncio.Semaphore(semaphore_count)
          self.stat_log = status_logger

          self.successes = []
          self.fails = []
          self.timeouts = []
          
          self.metadat = {
              'timeouts': 0,
              'error_rate': 0,
              'succesful_pulls_per_sec': 0,
              'timeouts_per_batch_ratio': 0
          }

          if sim_item_type.lower() == 'book':
               self.sim_fn = _parse_sim_books_page
          elif sim_item_type.lower() == 'author':
               self.sim_fn = _parse_sim_authors_page
          else:
               raise ValueError("sim_item_type must be in ['book', 'author']")


    async def _load_one_item(self,
                             session: aiohttp.ClientSession,
                             semaphore: asyncio.Semaphore,
                             identifier: str,
                             num_attempts: int = 1) -> Dict[str,Any]:
                '''
                load one Goodreads sim_item data unit; made for DB data collection step.
                
                :session: an aiohttp.ClientSession
                :semaphore: an asyncio.Semaphore
                :identifier: a sim_item ID
                :num_attempts: number of attempts (including initial attempt)
                '''
                res = {'sim_id': identifier, 'data': identifier, 'status': 'error'}    

                async with semaphore:
                    num_attempts = max(num_attempts, 1)
                    t_start = time.time()
                    
                    sim_item_url = f'https://www.goodreads.com/{self.sim_item_type}/similar/{identifier}'
                    for attempt in range(num_attempts):
                        try:
                            async with session.get(url=sim_item_url, headers=_rand_headers()) as resp:
                                txt = await resp.text()
                            item_dat = self.sim_fn(txt=txt)
                            res = {'sim_id': identifier, 'data': item_dat, 'status': 'success'}
                            break
                        
                        except asyncio.TimeoutError:
                            self.metadat['timeouts'] += 1
                            if (attempt + 1) == num_attempts:
                                self.stat_log.error('batch %s OUT OF RETRIES sim_id %s', self.batch_id, identifier) 
                                res = {'sim_id': identifier, 'data': identifier, 'status': 'timeout'}  # will pull again in the future
                                break
                            SLEEP_SCALAR = 1.5
                            sleep_time = (attempt + 1) ** SLEEP_SCALAR
                            await asyncio.sleep(sleep_time)
                            self.stat_log.info('batch %s RETRY sim_id %s %s', self.batch_id, self.sim_item_type, identifier)  

                        except Exception as er:
                            self.stat_log.error('batch %s ERR. sim_id %s %s: %s', self.batch_id, self.sim_item_type, identifier, er)
                            break   
                
                t_finished = time.time()
                t_elapsed = round(t_finished - t_start,3)
                self.stat_log.info('batch %s T.E. sim_id %s %s: %s sec.', self.batch_id, self.sim_item_type, identifier, t_elapsed)

                return res     
    

    async def load_the_batch(self,
                             session: aiohttp.ClientSession,
                             num_attempts: int = 1,
                             batch_delay: Optional[int] = None,
                             batch_size: Optional[int] = None) -> None:
        '''loads in batch of Goodreads item data.'''
        tasks = [self._load_one_item(session=session,
                                     semaphore=self.semaphore,
                                     identifier=item_id,
                                     num_attempts=num_attempts) for item_id in self.sim_item_ids]
        
        completed = 0
        batch_start = time.time()

        async for task in asyncio.as_completed(tasks):
            if batch_delay and batch_size:
                if completed > 0 and completed % batch_size == 0:
                        time.sleep(batch_delay)
            
            result = await task

            if result['status'] == 'success':
                self.successes.append(result)   # could be a full list of books/authors, or could be empty
            
            if result['status'] == 'timeout':
                self.timeouts.append(result['data'])    # will try again in the future
            
            if result['status'] == 'error':
                self.fails.append(result['data'])   # error for some other reason, try again in future
            
            completed += 1
        
        batch_end = time.time()
        batch_elapsed = round(batch_end - batch_start,3)
        success_rate = round(len(self.successes) / completed, 3)
        pulls_per_sec = round(completed / batch_elapsed, 3)
        
        self.stat_log.info('SUMMARY batch %s :: SM: %s :: BDL: %s :: TE: %s :: SR: %s :: PPS: %s :: ATP: %s',
                           self.batch_id, 
                           self.semaphore_count,
                           round(batch_delay, 3),
                           batch_elapsed,
                           success_rate,
                           pulls_per_sec,
                           completed)   # summary line, will be parsed in the future
        
        self.stat_log.info('T.E. batch %s: %s sec.', self.batch_id, batch_elapsed) 
        self.stat_log.info('SUCCESS RATE batch %s: %s', self.batch_id, success_rate)
        self.stat_log.info('PULLS PER SEC batch %s: %s', self.batch_id, pulls_per_sec)
        self.stat_log.info('batch %s TIMED-OUT %ss: %s', self.batch_id, self.sim_item_type, self.timeouts)

        err_rate = 1 - success_rate
        succ_pull_per_sec = round(len(self.successes) / batch_elapsed, 3)
        self.metadat['error_rate'] = err_rate
        self.metadat['succesful_pulls_per_sec'] = succ_pull_per_sec

        self.metadat['timeouts_per_batch_ratio'] = round(self.metadat['timeouts'] / completed, 3)
        

    @abstractmethod
    def insert_batch_into_db(self) -> None:
        '''insert results into DB'''
        pass


# used to pull sim_books and update sim_books col in alexandria
class SimBooksPuller(SimItemsPuller):
    '''pull similar-books for a given book, store in a db'''
    def __init__(self, 
                 batch_id: Union[int,str], 
                 cursor: psycopg.Cursor,  
                 sim_book_ids: Iterable[str], 
                 semaphore_count: int, 
                 status_logger: logging.Logger):
        '''pull Goodreads similar_book data.
        
        :batch_id: batch identifier; used for logging
        :param cursor: a psycopg Cursor object
        :param sim_book_ids: an iterable of Goodreads similar-book IDs
        :param semaphore_count: number of maximum concurrent coroutines
        :param status_logger: a Logger object to record progress/status/issues
        '''
        super().__init__(batch_id=batch_id, 
                         cursor=cursor, 
                         sim_item_type='book', 
                         sim_item_ids=sim_book_ids, 
                         semaphore_count=semaphore_count, 
                         status_logger=status_logger)
    

    def insert_batch_into_db(self) -> None:
        '''update sim_books column
        
        1. create table (if not exists) to insert the sim_id, and text array of similar book IDs
        2. insert tuples. In case of NA values, an empty array is inserted
        3. update main alexandria table and set sim_books column, join the newly created table and alexandria on sim_id
        '''
        dat2insert = []
        for id_ in self.successes:
            tpl2insert = (id_['sim_id'], id_['data'])
            dat2insert.append(tpl2insert)
        
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
        self.cursor.executemany(insert_into_sim_books_query, dat2insert)

        update_and_set_query = '''
                                UPDATE alexandria alx
                                SET sim_books = sb.sim_books
                                FROM sim_books sb
                                WHERE alx.sim_books_url_id = sb.sim_id
                               '''
        self.cursor.execute(update_and_set_query)


# used to pull sim_authors and update sim_authors col in pound
class SimAuthorsPuller(SimItemsPuller):
    '''pull similar-authors for a given author, store in a db'''
    def __init__(self, 
                 batch_id: Union[int,str], 
                 cursor: psycopg.Cursor,  
                 sim_author_ids: Iterable[str], 
                 semaphore_count: int, 
                 status_logger: logging.Logger):
        '''pull Goodreads similar_author data.
        
        :batch_id: batch identifier; used for logging
        :param cursor: a psycopg Cursor object
        :param sim_author_ids: an iterable of Goodreads similar-author IDs
        :param semaphore_count: number of maximum concurrent coroutines
        :param status_logger: a Logger object to record progress/status/issues
        '''
        super().__init__(batch_id=batch_id, 
                         cursor=cursor, 
                         sim_item_type='author', 
                         sim_item_ids=sim_author_ids, 
                         semaphore_count=semaphore_count, 
                         status_logger=status_logger)
    
    def insert_batch_into_db(self) -> None:
        '''update sim_authors column
        
        1. create table (if not exists) to insert the sim_id, and text array of similar author IDs
        2. insert tuples. In case of NA values, an empty array is inserted
        3. update main pound table and set sim_authrs column, join the newly created table and pound on sim_id == author_id
        '''
        dat2insert = []
        for id_ in self.successes:
            tpl2insert = (id_['sim_id'], id_['data'])
            dat2insert.append(tpl2insert)
        
        create_table_query = '''
                            CREATE TABLE IF NOT EXISTS sim_authors
                            (
                                sim_id text PRIMARY KEY,
                                sim_authors text[]
                            )
                            '''
        self.cursor.execute(create_table_query)

        truncate_the_table_query = '''TRUNCATE TABLE sim_authors'''
        self.cursor.execute(truncate_the_table_query)

        insert_into_sim_authors_query = '''
                                        INSERT INTO sim_authors 
                                            (sim_id,
                                             sim_authors)
                                        VALUES
                                            (%s,
                                             %s)
                                        ON CONFLICT DO NOTHING -- this shouldn't ever happen, but just in case
                                      '''
        self.cursor.executemany(insert_into_sim_authors_query, dat2insert)

        update_and_set_query = '''
                                UPDATE pound pnd
                                SET sim_authors = sa.sim_authors
                                FROM sim_authors sa
                                WHERE pnd.author_id = sa.sim_id
                               '''
        self.cursor.execute(update_and_set_query)
        self.stat_log.info('batch %s DB INSERT %s TUPLES', self.batch_id, len(dat2insert))

            