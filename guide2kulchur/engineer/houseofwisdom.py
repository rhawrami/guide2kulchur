import re
import asyncio
import time
import logging
from typing import (Optional, 
                    Dict, 
                    Union, 
                    Iterable, 
                    Any)

import aiohttp
import psycopg
from psycopg.types.json import Jsonb

from guide2kulchur.privateer.alexandria import Alexandria


def _jsonb_or_null(obj: Optional[dict]) -> Optional[Jsonb]:
    '''returns either psycopg Jsonb object, or None'''
    if isinstance(obj, dict):
        return Jsonb(obj)
    else:
        return None


class HouseOfWisdom(Alexandria):
    '''Goodreads book data collector, with some minor changes'''
    def __init__(self):
        super().__init__()

    
    def get_similar_books_id(self) -> Optional[str]:
        '''Returns the "Similar Books" URL ID for a given book.'''
        self._confirm_loaded()
        if bklst := self._soup.find('div', 
                                    class_='BookDiscussions__list'):
            if quote_tag := bklst.find_all('a',class_='DiscussionCard'):    # use this to get proper serial id
                if quote_url := quote_tag[0].get('href'):   # the serial id changes from main page to similar page
                    if similar_books_id := re.search(r'\d+', quote_url):
                        return similar_books_id.group(0)    # the above conditional should always eval True, but just in case
        return None
    

    def get_all_data(self) -> Dict[str,Any]:
        '''returns collection of data from loaded Goodreads book in dict format; meant for collection step.'''
        self._confirm_loaded()
        attr_fn_map = {
            'url': lambda: self.book_url,
            'id': self.get_id,
            'title': self.get_title,
            'author': self.get_author_name,
            'author_id': self.get_author_id,
            'isbn': self.get_isbn,
            'language': self.get_language,
            'image_url': self.get_image_url,
            'description': self.get_description,
            'rating': self.get_rating,
            'rating_distribution': self.get_rating_dist,
            'rating_count': self.get_rating_count,
            'review_count': self.get_review_count,
            'top_genres': self.get_top_genres,
            'currently_reading': self.get_currently_reading,
            'want_to_read': self.get_want_to_read,
            'page_length': self.get_page_length,
            'first_published': self.get_first_published,
            'similar_books_id': self.get_similar_books_id
        }
        
        bk_dict = {}
        for attr,fn in attr_fn_map.items():
            bk_dict[attr] = fn()
        return bk_dict 


class BatchBookPuller:
    '''pull a batch of Goodreads books, log results, load into database'''
    def __init__(self,
                 batch_id: str,
                 cursor: psycopg.Cursor,
                 book_ids: Iterable[str],
                 semaphore_count: int,
                 status_logger: logging.Logger):
          '''pull Goodreads book data.
          
          :batch_id: batch identifer; used for logging
          :param cursor: a psycopg Cursor object
          :param book_ids: an iterable of Goodreads book IDs
          :param semaphore_counr: number of maximum concurrent coroutines
          :param status_logger: a Logger object to record progress/status/issues
          '''
          self.batch_id = batch_id
          self.cursor = cursor
          self.book_ids = book_ids
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


    async def _load_one_book(self,
                             session: aiohttp.ClientSession,
                             semaphore: asyncio.Semaphore,
                             identifer: str,
                             num_attempts: int = 1,
                             see_progress: bool = True) -> Union[Dict[str,Any],str]:
                '''
                load one Goodreads book; made for DB data collection step.
                
                :session: an aiohttp.ClientSession
                :semaphore: an asyncio.Semaphore
                :identifer: a book ID or URL
                :num_attempts: number of attempts (including initial attempt)
                :see_progress: view progress for each book pull

                returns Dict of book data if successful, book ID string if failure
                '''
                res = identifer

                async with semaphore:
                    num_attempts = max(num_attempts, 1)
                    t_start = time.time()
                    for attempt in range(num_attempts):
                        try:
                            hOw = HouseOfWisdom()
                            await hOw.load_book_async(session=session,
                                                      book_identifier=identifer,
                                                      see_progress=see_progress)
                            
                            res = hOw.get_all_data()
                            break
                        
                        except asyncio.TimeoutError:
                            self.metadat['timeouts'] += 1
                            if (attempt + 1) == num_attempts:
                                self.logger.error('batch %s OUT OF RETRIES %s', self.batch_id, identifer) 
                                res = (identifer,)  # will pull again in the future
                                break
                            SLEEP_SCALAR = 1.5
                            sleep_time = (attempt + 1) ** SLEEP_SCALAR
                            await asyncio.sleep(sleep_time)
                            self.stat_log.info('batch %s RETRY book %s', self.batch_id, identifer)  

                        except Exception as er:
                            self.stat_log.error('batch %s ERR. book %s: %s', self.batch_id, identifer, er)
                            break   
                
                t_finished = time.time()
                t_elapsed = round(t_finished - t_start,3)
                self.stat_log.info('batch %s T.E. book %s: %s sec.', self.batch_id, identifer, t_elapsed)

                return res     
    

    async def load_the_batch(self,
                             session: aiohttp.ClientSession,
                             num_attempts: int = 1,
                             see_progress: bool = True,
                             batch_delay: Optional[int] = None,
                             batch_size: Optional[int] = None) -> None:
        '''loads in batch of Goodreads book data.'''
        tasks = [self._load_one_book(session=session,
                                        semaphore=self.semaphore,
                                        identifer=bk_id,
                                        num_attempts=num_attempts,
                                        see_progress=see_progress) for bk_id in self.book_ids]
        
        completed = 0
        batch_start = time.time()

        async for task in asyncio.as_completed(tasks):
            if batch_delay and batch_size:
                if completed > 0 and completed % batch_size == 0:
                        time.sleep(batch_delay)
            
            result = await task
            if isinstance(result,dict):
                self.successes.append(result)   # successful pulls

            elif isinstance(result, str):
                self.fails.append(result)   # error IDs

            else:
                self.timeouts.append(result[0]) # in the case of a timeout-tuple
            completed += 1
        
        batch_end = time.time()
        batch_elapsed = round(batch_end - batch_start,3)
        success_rate = round(len(self.successes) / completed, 3)
        
        self.stat_log.info('T.E. batch %s: %s sec.', self.batch_id, batch_elapsed) 
        self.stat_log.info('SUCCESS RATE batch %s: %s', self.batch_id, success_rate)
        self.stat_log.info('batch %s FAILED books: %s', self.batch_id, self.fails)
        self.stat_log.info('batch %s TIMED-OUT books: %s', self.batch_id, self.timeouts)

        err_rate = 1 - success_rate
        succ_pull_per_sec = round(len(self.successes) / batch_elapsed, 3)
        self.metadat['error_rate'] = err_rate
        self.metadat['succesful_pulls_per_sec'] = succ_pull_per_sec

        self.metadat['timeouts_per_batch_ratio'] = round(self.metadat['timeouts'] / completed, 3)


    def insert_failed_ids_into_db(self):
        '''inserts failed book IDs into error_id table for future reference'''
        if not self.fails:
            return None
        
        failed_ids_statement = '''
                                INSERT INTO error_id 
                                    (item_id, item_type)
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING
                               '''
        fails_to_insert = [(fail_id, 'book') for fail_id in self.fails]
        self.cursor.executemany(failed_ids_statement, fails_to_insert)
        

    def insert_batch_into_db(self) -> None:
        '''insert results into DB'''
        dat_to_insert = []

        for bk in self.successes:
            for field,val in bk.items():
                # ensure ratings are between 1 and 5
                if field == 'rating' and isinstance(val, (int,float)) and (val > 5 or val < 1):
                    bk[field] = None 
                # ensure numeric types are positive
                if isinstance(val, (int,float)) and val < 0:
                    bk[field] = None

            dat_as_tuple = (bk['id'],
                            bk['title'],
                            bk['author'],
                            bk['author_id'],
                            bk['isbn'],
                            bk['language'],
                            bk['description'],
                            bk['image_url'],
                            bk['rating'],
                            _jsonb_or_null(bk['rating_distribution']),
                            bk['rating_count'],
                            bk['review_count'],
                            bk['top_genres'],
                            bk['currently_reading'],
                            bk['want_to_read'],
                            bk['first_published'],
                            bk['page_length'],
                            bk['similar_books_id'])
            dat_to_insert.append(dat_as_tuple)
        
        insert_query =  '''
                            INSERT INTO alexandria 
                               (book_id, 
                                title, 
                                author, 
                                author_id,
                                isbn,
                                lang,
                                descr,
                                img_url,
                                rating,
                                rating_dist,
                                rating_count,
                                review_count,
                                top_genres,
                                currently_reading,
                                want_to_read,
                                first_published,
                                page_length,
                                sim_books_url_id)
                            VALUES 
                                (%s, %s, %s, %s, %s, %s, 
                                 %s, %s, %s, %s, %s, %s,
                                 %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        '''
        t_start = time.time()
        self.cursor.executemany(insert_query, dat_to_insert)
        t_end = time.time()
        t_e = round(t_end - t_start, 3)
        self.stat_log.info('batch %s DB INSERT %s tuples: %s sec', self.batch_id, len(dat_to_insert), t_e)


                 