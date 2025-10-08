import asyncio
import time
import logging
from abc import ABC, abstractmethod
from typing import (Optional, 
                    Dict, 
                    Union, 
                    Iterable, 
                    Any)

import aiohttp
import psycopg

from guide2kulchur.engineer.recruits import (HouseOfWisdom, 
                                             Dante, 
                                             FalseBardiya,
                                             _jsonb_or_null)


class BatchItemPuller(ABC):
    '''pull a batch of Goodreads item (book|author|user) data, log results, load into database'''
    def __init__(self,
                 batch_id: Union[int,str],
                 cursor: psycopg.Cursor,
                 item_type: str,
                 item_ids: Iterable[str],
                 semaphore_count: int,
                 status_logger: logging.Logger):
          '''pull Goodreads item data.
          
          :batch_id: batch identifier; used for logging
          :param cursor: a psycopg Cursor object
          :param item_type: Goodreads item data type (book|author|user) 
          :param item_ids: an iterable of Goodreads item (book|author|user) IDs
          :param semaphore_counr: number of maximum concurrent coroutines
          :param status_logger: a Logger object to record progress/status/issues
          '''
          self.batch_id = batch_id
          self.cursor = cursor
          self.item_type = item_type
          self.item_ids = item_ids
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

          if item_type.lower() == 'book':
               self.item_puller = HouseOfWisdom
          elif item_type.lower() == 'author':
               self.item_puller = Dante
          elif item_type.lower() == 'user':
               self.item_puller = FalseBardiya
          else:
               raise ValueError("item_type must be in ['book', 'author', 'user']")


    async def _load_one_item(self,
                             session: aiohttp.ClientSession,
                             semaphore: asyncio.Semaphore,
                             identifier: str,
                             num_attempts: int = 1,
                             see_progress: bool = True) -> Union[Dict[str,Any],str]:
                '''
                load one Goodreads item; made for DB data collection step.
                
                :session: an aiohttp.ClientSession
                :semaphore: an asyncio.Semaphore
                :identifier: a book ID or URL
                :num_attempts: number of attempts (including initial attempt)
                :see_progress: view progress for each item pull
                '''
                res = {'data': identifier, 'status': 'error'}    # assume err

                async with semaphore:
                    num_attempts = max(num_attempts, 1)
                    t_start = time.time()
                    for attempt in range(num_attempts):
                        try:
                            loaded_item = await self.item_puller().load_it_async(session=session,
                                                                                 item_id=identifier,
                                                                                 see_progress=see_progress)
                            
                            item_dat = loaded_item.get_all_data()
                            res = {'data': item_dat, 'status': 'success'}
                            break
                        
                        except asyncio.TimeoutError:
                            self.metadat['timeouts'] += 1
                            if (attempt + 1) == num_attempts:
                                self.stat_log.error('batch %s OUT OF RETRIES %s', self.batch_id, identifier) 
                                res = {'data': item_dat, 'status': 'timeout'}  # will pull again in the future
                                break
                            SLEEP_SCALAR = 1.5
                            sleep_time = (attempt + 1) ** SLEEP_SCALAR
                            await asyncio.sleep(sleep_time)
                            self.stat_log.info('batch %s RETRY %s %s', self.batch_id, self.item_type, identifier)  

                        except Exception as er:
                            self.stat_log.error('batch %s ERR. %s %s: %s', self.batch_id, self.item_type, identifier, er)
                            break   
                
                t_finished = time.time()
                t_elapsed = round(t_finished - t_start,3)
                self.stat_log.info('batch %s T.E. %s %s: %s sec.', self.batch_id, self.item_type, identifier, t_elapsed)

                return res     
    

    async def load_the_batch(self,
                             session: aiohttp.ClientSession,
                             num_attempts: int = 1,
                             see_progress: bool = True,
                             batch_delay: Optional[int] = None,
                             batch_size: Optional[int] = None) -> None:
        '''loads in batch of Goodreads item data.'''
        tasks = [self._load_one_item(session=session,
                                     semaphore=self.semaphore,
                                     identifier=item_id,
                                     num_attempts=num_attempts,
                                     see_progress=see_progress) for item_id in self.item_ids]
        
        completed = 0
        batch_start = time.time()

        async for task in asyncio.as_completed(tasks):
            if batch_delay and batch_size:
                if completed > 0 and completed % batch_size == 0:
                        time.sleep(batch_delay)
            
            result = await task
            
            if result['status'] == 'success':
                self.successes.append(result['data'])
            
            if result['status'] == 'timeout':
                self.timeouts.append(result['data'])
            
            if result['status'] == 'error':
                self.fails.append(result['data'])

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
        self.stat_log.info('batch %s FAILED %ss: %s', self.batch_id, self.item_type, self.fails)
        self.stat_log.info('batch %s TIMED-OUT %ss: %s', self.batch_id, self.item_type, self.timeouts)

        err_rate = 1 - success_rate
        succ_pull_per_sec = round(len(self.successes) / batch_elapsed, 3)
        self.metadat['error_rate'] = err_rate
        self.metadat['succesful_pulls_per_sec'] = succ_pull_per_sec

        self.metadat['timeouts_per_batch_ratio'] = round(self.metadat['timeouts'] / completed, 3)


    def insert_failed_ids_into_db(self):
        '''inserts failed item IDs into error_id table for future reference'''
        if not self.fails:
            return None
        
        failed_ids_statement = '''
                                INSERT INTO error_id 
                                    (item_id, item_type)
                                VALUES (%s, %s)
                                ON CONFLICT DO NOTHING
                               '''
        fails_to_insert = [(fail_id, self.item_type) for fail_id in self.fails]
        self.cursor.executemany(failed_ids_statement, fails_to_insert)
        

    @abstractmethod
    def insert_batch_into_db(self) -> None:
        '''insert results into DB'''
        pass

# BatchBookPuller
# pulling books from Goodreads in defined batches
class BatchBookPuller(BatchItemPuller):
    '''pull a batch of Goodreads books, log results, load into database'''
    def __init__(self, 
                 batch_id: Union[int,str],
                 cursor: psycopg.Cursor,
                 book_ids: Iterable[str],
                 semaphore_count: int,
                 status_logger: logging.Logger):
        '''pull Goodreads book data.
          
        :batch_id: batch identifier; used for logging
        :param cursor: a psycopg Cursor object
        :param book_ids: an iterable of Goodreads book IDs
        :param semaphore_count: number of maximum concurrent coroutines
        :param status_logger: a Logger object to record progress/status/issues
        '''
        super().__init__(batch_id=batch_id, 
                         cursor=cursor, 
                         item_type='book', 
                         item_ids=book_ids, 
                         semaphore_count=semaphore_count, 
                         status_logger=status_logger)
    
    
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
        

# BatchAuthorPuller
# pulling authors from Goodreads in defined batches
class BatchAuthorPuller(BatchItemPuller):
    '''pull a batch of Goodreads authors, log results, load into database'''
    def __init__(self, 
                 batch_id: Union[int,str],
                 cursor: psycopg.Cursor,
                 author_ids: Iterable[str],
                 semaphore_count: int,
                 status_logger: logging.Logger):
        '''pull Goodreads author data.
          
        :batch_id: batch identifier; used for logging
        :param cursor: a psycopg Cursor object
        :param author_ids: an iterable of Goodreads author IDs
        :param semaphore_count: number of maximum concurrent coroutines
        :param status_logger: a Logger object to record progress/status/issues
        '''
        super().__init__(batch_id=batch_id, 
                         cursor=cursor, 
                         item_type='author', 
                         item_ids=author_ids, 
                         semaphore_count=semaphore_count, 
                         status_logger=status_logger)
    

    def insert_batch_into_db(self) -> None:
        '''insert results into DB'''
        dat_to_insert = []

        for athr in self.successes:
            for field,val in athr.items():
                # ensure ratings are between 1 and 5
                if field == 'rating' and isinstance(val, (int,float)) and (val > 5 or val < 1):
                    athr[field] = None 
                # ensure numeric types are positive
                if isinstance(val, (int,float)) and val < 0:
                    athr[field] = None

            dat_as_tuple = (athr['author_id'],
                            athr['author_name'],
                            athr['description'],
                            athr['image_url'],
                            athr['birth_place'],
                            athr['birth'],
                            athr['death'],
                            athr['top_genres'],
                            athr['influences'],
                            athr['book_sample'],
                            athr['quotes_sample'],
                            athr['rating'],
                            athr['rating_count'],
                            athr['review_count'],
                            athr['follower_count'])
            dat_to_insert.append(dat_as_tuple)
        
        insert_query =  '''
                            INSERT INTO pound
                               (author_id, 
                                author_name, 
                                descr, 
                                img_url,
                                birth_place,
                                birth,
                                death,
                                top_genres,
                                influences,
                                book_sample,
                                quotes_sample,
                                rating,
                                rating_count,
                                review_count,
                                follower_count)
                            VALUES 
                                (%s, %s, %s, %s, %s, 
                                 %s, %s, %s, %s, %s,
                                 %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        '''
        t_start = time.time()
        self.cursor.executemany(insert_query, dat_to_insert)
        t_end = time.time()
        t_e = round(t_end - t_start, 3)
        self.stat_log.info('batch %s DB INSERT %s tuples: %s sec', self.batch_id, len(dat_to_insert), t_e)


# BatchUserPuller
# pulling users from Goodreads in defined batches
class BatchUserPuller(BatchItemPuller):
    '''pull a batch of Goodreads users, log results, load into database'''
    def __init__(self, 
                 batch_id: Union[int,str],
                 cursor: psycopg.Cursor,
                 user_ids: Iterable[str],
                 semaphore_count: int,
                 status_logger: logging.Logger):
        '''pull Goodreads user data.
          
        :batch_id: batch identifier; used for logging
        :param cursor: a psycopg Cursor object
        :param user_ids: an iterable of Goodreads user IDs
        :param semaphore_count: number of maximum concurrent coroutines
        :param status_logger: a Logger object to record progress/status/issues
        '''
        super().__init__(batch_id=batch_id, 
                         cursor=cursor, 
                         item_type='user', 
                         item_ids=user_ids, 
                         semaphore_count=semaphore_count, 
                         status_logger=status_logger)
    

    def insert_batch_into_db(self) -> None:
        '''insert results into DB'''
        dat_to_insert = []

        for athr in self.successes:
            for field,val in athr.items():
                # ensure ratings are between 1 and 5
                if field == 'rating' and isinstance(val, (int,float)) and (val > 5 or val < 1):
                    athr[field] = None 
                # ensure numeric types are positive
                if isinstance(val, (int,float)) and val < 0:
                    athr[field] = None

            dat_as_tuple = (athr['user_id'],
                            athr['user_name'],
                            athr['image_url'],
                            athr['rating'],
                            athr['rating_count'],
                            athr['review_count'],
                            athr['favorite_genres'],
                            athr['follower_count'],
                            athr['friend_count'],
                            athr['currently_reading_sample_books'],
                            athr['currently_reading_sample_authors'],
                            athr['featured_shelf_sample_books'],
                            athr['shelves'],
                            athr['followings_sample_users'],
                            athr['followings_sample_authors'],
                            athr['quotes_sample_strings'],
                            athr['quotes_sample_author_ids'],
                            athr['friends_sample'],
                            athr['currently_reading_update_time'])
            dat_to_insert.append(dat_as_tuple)
        
        insert_query =  '''
                            INSERT INTO false_dmitry
                               (user_id, 
                                user_name, 
                                img_url,
                                rating,
                                rating_count,
                                review_count,
                                favorite_genres,
                                follower_count,
                                friend_count,
                                currently_reading_sample_books,
                                currently_reading_sample_authors,
                                featured_shelf_sample_books,
                                shelf_names,
                                followings_sample_users,
                                followings_sample_authors,
                                quotes_sample_strings,
                                quotes_sample_author_ids,
                                friends_sample,
                                cr_recent_update
                                )
                            VALUES 
                                (%s, %s, %s, %s, %s, %s,
                                 %s, %s, %s, %s, %s, %s,
                                 %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT DO NOTHING
                        '''
        t_start = time.time()
        self.cursor.executemany(insert_query, dat_to_insert)
        t_end = time.time()
        t_e = round(t_end - t_start, 3)
        self.stat_log.info('batch %s DB INSERT %s tuples: %s sec', self.batch_id, len(dat_to_insert), t_e)
        