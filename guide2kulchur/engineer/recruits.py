import os
import sys
import logging
import re
import asyncio
import urllib.parse
from logging.handlers import RotatingFileHandler
from typing import (Tuple, 
                    Dict,
                    List, 
                    Any,
                    Optional)

import aiohttp
from psycopg.types.json import Jsonb

from guide2kulchur.privateer.alexandria import Alexandria
from guide2kulchur.privateer.pound import Pound
from guide2kulchur.privateer.falsedmitry import FalseDmitry


def gen_logger(name: str,
               name_abbr: str,
               max_bytes_per_log: int,
               max_backups: int) -> logging.Logger:
    '''set up logger object, return logger with separate progress/error tracking
    
    :param name: name of logger and of directory where logs will be stored
    :param name_abbr: name abbreviation, used for log file names
    :param max_bytes_per_log: max byte count per log file
    :param max_backups: max number of log files
    '''
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)

    # make dirs
    os.makedirs('logs', exist_ok=True)
    os.makedirs(os.path.join('logs', name), exist_ok=True)

    # progress statements; e.g., time-to-complete batch #5
    PROG_PATH = os.path.join('logs', name, f'{name_abbr}_prog.log')
    prog_handler = RotatingFileHandler(filename=PROG_PATH,
                                       maxBytes=max_bytes_per_log,
                                       backupCount=max_backups)
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
    ERR_PATH = os.path.join('logs', name, f'{name_abbr}_err.log')
    err_handler = RotatingFileHandler(filename=ERR_PATH,
                                      maxBytes=max_bytes_per_log,
                                      backupCount=max_backups)
    err_handler.setLevel(logging.ERROR)
    err_fmt = logging.Formatter(fmt='%(asctime)s %(levelname)s := %(message)s',
                                datefmt='%m-%d-%Y %H:%M:%S')
    err_handler.setFormatter(err_fmt)

    logger.addHandler(prog_handler)
    logger.addHandler(err_handler)
    logger.addHandler(stream_handler)
    return logger
    

def update_sem_and_delay(current_sem_count: int,
                         current_sub_batch_delay: int,
                         timeouts_per_batch_ratio: float,
                         cfg: Optional[Dict[str,int]] = None) -> Tuple[int,int]:
    '''takes in batch metadata, returns new SEMAPHORE COUNT & SUB_BATCH_DELAY
    
    :param current_sem_count: current semaphore count
    :param current_sub_batch_delay: current delay in between sub-batches
    :param timeouts_per_batch_ratio: ratio of timeouts per batch pulls; e.g., # timeouts / # tasks
    :param cfg: sem/delay config; includes keys: MIN_SEM, MAX_SEM, MIN_DELAY, MAX_DELAY, RATIO_THRESHOLD, DELAY_DELTA
    '''
    if not cfg:
        cfg = {
            'MIN_SEM': 2,
            'MAX_SEM': 9,
            'MIN_DELAY': .25,
            'MAX_DELAY': 5,
            'RATIO_THRESHOLD': .05,
            'DELAY_DELTA': .1
        }

    scalar = -1 if timeouts_per_batch_ratio > cfg['RATIO_THRESHOLD'] else 1
    
    new_sem_count = (lambda x: sorted([cfg['MIN_SEM'], 
                                       current_sem_count + x, 
                                       cfg['MAX_SEM']])[1])(scalar)
    
    new_delay_count = (lambda x: sorted([cfg['MIN_DELAY'], 
                                         current_sub_batch_delay - cfg['DELAY_DELTA'] * x, 
                                         cfg['MAX_DELAY']])[1])(scalar)
    
    return new_sem_count, new_delay_count


class HouseOfWisdom(Alexandria):
    '''Goodreads book data collector, with some minor changes'''
    def __init__(self):
        super().__init__()


    async def load_it_async(self,
                            session: aiohttp.ClientSession,
                            item_id: str,
                            see_progress: bool):
        '''load the item.'''
        await self.load_book_async(session=session,
                                   book_identifier=item_id,
                                   see_progress=see_progress)
        return self

    
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
    

class Dante(Pound):
    '''Goodreads author data collector, with some minor changes'''
    def __init__(self):
        super().__init__()


    async def load_it_async(self,
                            session: aiohttp.ClientSession,
                            item_id: str,
                            see_progress: bool) -> 'Dante':
        '''load the item.'''
        await self.load_author_async(session=session,
                                     author_identifier=item_id,
                                     see_progress=see_progress)
        return self
    

    def get_influences(self) -> Optional[List[str]]:
        '''get influences, convert to list'''
        influences_og = super().get_influences()
        if not influences_og:
            return None
        return [author['id'] 
                for author 
                in influences_og 
                if author['id'] and isinstance(author['id'], str)]


    def get_books_sample(self) -> Optional[List[str]]:
        '''get influences, convert to list'''
        samples_og = super().get_books_sample()
        if not samples_og:
            return None
        return [book['id'] 
                for book 
                in samples_og 
                if book['id'] and isinstance(book['id'], str)]
    

    def get_all_data(self) -> Dict[str,Any]:
        '''returns collection of data from loaded Goodreads author in dict format; meant for collection step.'''
        self._confirm_loaded()
        attr_fn_map = {
            'author_id': self.get_id,
            'author_name': self.get_name,
            'description': self.get_description,
            'image_url': self.get_image_url,
            'birth_place': self.get_birth_place,
            'birth': self.get_birth_date,
            'death': self.get_death_date,
            'top_genres': self.get_top_genres,
            'influences': self.get_influences,
            'book_sample': self.get_books_sample,
            'quotes_sample': self.get_quotes_sample,
            'rating': self.get_rating,
            'rating_count': self.get_rating_count,
            'review_count': self.get_review_count,
            'follower_count': self.get_follower_count
        }
        
        athr_dict = {}
        for attr,fn in attr_fn_map.items():
            athr_dict[attr] = fn()
        return athr_dict 
    

class FalseBardiya(FalseDmitry):
    '''Goodreads user data collector, with some minor changes'''
    def __init__(self):
        super().__init__()


    async def load_it_async(self,
                            session: aiohttp.ClientSession,
                            item_id: str,
                            see_progress: bool) -> 'FalseBardiya':
        '''load the item.'''
        await self.load_user_async(session=session,
                                   user_identifier=item_id,
                                   see_progress=see_progress)
        return self
    

    def get_shelves(self) -> Optional[List[str]]:
        '''return list of user's shelve names'''
        shelf_names = []
        if shelves_container := self._info_left.find('div', {'id': 'shelves'}):
            for shelf_col in shelves_container.find_all('div', class_='shelfContainer'):
                for shelf in shelf_col.find_all('a', class_='userShowPageShelfListItem'):
                    # mind the mess below future me, you were having trouble with filtering
                    # by the anchor text, so you decided to use the much easier href link
                    # you also replace hyphens in shelf names with spaces
                    if (name_link := shelf.get('href')) and (re.search(r'shelf=.*$', name_link)):
                        name = re.search(r'shelf=.*$', name_link).group(0)
                        name = urllib.parse.unquote(name)   # since we're pulling from href, some strings will get percent-encoded
                        name = re.sub(r'^shelf=|-', '', name)
                        if name:
                            shelf_names.append(name)    # in case of weird regex subbing
        
        return shelf_names if shelf_names else None


    def get_featured_shelf_sample(self):
        '''return sample list of books on a user's featured shelf'''
        featured_shelf_og = super().get_featured_shelf()
        return [re.sub(r'\D', '', bk['id'])
                for bk
                # This is a bit of a mess, but featured_shelf_og returns a 
                # singe key-value dictionary, with the value being a dict of book data
                in list(featured_shelf_og.values())[0]  
                if bk['id'] and isinstance(bk['id'], str)] if featured_shelf_og else None


    def get_friends_sample(self) -> Optional[List[str]]:
        '''return sample list of user's friends'''
        friends_sample_og = super().get_friends_sample()
        return [re.sub(r'\D', '', user['id'])
                for user
                in friends_sample_og
                if user['id'] and isinstance(user['id'], str)] if friends_sample_og else None
    

    def get_followings_sample(self,
                              item_type: str) -> Optional[List[str]]:
        '''return sample list of user's followings
        
        :param item_type: either author or user
        '''
        self._confirm_loaded()
        boxes = self._info_right.find_all('div',class_='clearFloats bigBox')
        for box in boxes:
            title = box.find('a').text
            if re.search(r'.*is Following',title):
                follow_box = box.find('div',class_='bigBoxContent containerWithHeaderContent')
                follow_dat = []
                for follower in follow_box.find_all('div'):
                    flwr = follower.find('a')
                    if flwr:
                        flwr_id = flwr['href']
                        if re.search(r'\/user\/show', flwr_id):
                            flwr_type = 'user'
                        elif re.search(r'\/author\/show', flwr_id):
                            flwr_type = 'author'
                        flwr_id = re.sub(r'^.*show\/|-.*$|\.*$|\D','',flwr_id)
                        flwr_dat = {
                            'id': flwr_id,
                            'type': flwr_type
                        }
                        follow_dat.append(flwr_dat)
                return [re.sub(r'[a-z]|[A-Z]|\.|_', '', following['id'])
                        for following
                        in follow_dat
                        if following['type'] == item_type]
        return None
    

    def get_followings_sample_users(self) -> Optional[List[str]]:
        '''convenience function for get_followings_sample to pull only users'''
        dat = self.get_followings_sample(item_type='user')
        return dat if dat else None
    

    def get_followings_sample_authors(self) -> Optional[List[str]]:
        '''convenience function for get_followings_sample to pull only authors'''
        dat = self.get_followings_sample(item_type='author')
        return dat if dat else None


    def get_currently_reading_sample_books(self) -> Optional[List[str]]:
        '''return list of books that user is currently reading'''
        currently_reading_og = super().get_currently_reading_sample()
        if not currently_reading_og:
            return None
        return [re.sub(r'\D', '', bk['id'])
                for bk
                in currently_reading_og
                if bk['id'] and isinstance(bk['id'], str)] if currently_reading_og else None
    

    def get_currently_reading_sample_authors(self) -> Optional[List[str]]:
        '''return list of authors that user is currently reading'''
        currently_reading_og = super().get_currently_reading_sample()
        if not currently_reading_og:
            return None
        return [re.sub(r'\D', '', bk['author_id'])
                for bk
                in currently_reading_og
                if bk['author_id'] and isinstance(bk['author_id'], str)] 
    

    def get_quotes_sample_strings(self) -> Optional[List[str]]:
        '''return list of quote strings'''
        quotes_sample_og = super().get_quotes_sample()
        if not quotes_sample_og:
            return None
        return [qt['quote']
                for qt
                in quotes_sample_og
                if qt['quote'] and isinstance(qt['quote'], str)]
    

    def get_quotes_sample_author_ids(self) -> Optional[List[str]]:
        '''return list of author IDs quoted'''
        quotes_sample_og = super().get_quotes_sample()
        if not quotes_sample_og:
            return None
        return [re.sub(r'\D', '', qt['author_id'])  # in case of names included with ID
                for qt
                in quotes_sample_og
                if qt['author_id'] and isinstance(qt['author_id'], str)]
    
    
    def get_all_data(self) -> Dict[str,Any]:
        '''returns collection of data from loaded Goodreads user in dict format; meant for collection step.'''
        self._confirm_loaded()
        attr_fn_map = {
            'user_id': self.get_id,
            'user_name': self.get_name,
            'image_url': self.get_image_url,
            'rating': self.get_rating,
            'rating_count': self.get_rating_count,
            'review_count': self.get_review_count,
            'favorite_genres': self.get_favorite_genres,
            'currently_reading_sample_books': self.get_currently_reading_sample_books,
            'currently_reading_sample_authors': self.get_currently_reading_sample_authors,
            'shelves': self.get_shelves,
            'featured_shelf_sample_books': self.get_featured_shelf_sample,
            'follower_count': self.get_follower_count,
            'friend_count': self.get_friend_count,
            'friends_sample': self.get_friends_sample,
            'followings_sample_users': self.get_followings_sample_users,
            'followings_sample_authors': self.get_followings_sample_authors,
            'quotes_sample_strings': self.get_quotes_sample_strings,
            'quotes_sample_author_ids': self.get_quotes_sample_author_ids
        }
        
        usr_dict = {}
        for attr,fn in attr_fn_map.items():
            usr_dict[attr] = fn()
        return usr_dict 


def _jsonb_or_null(obj: Optional[dict]) -> Optional[Jsonb]:
    '''returns either psycopg Jsonb object, or None'''
    if isinstance(obj, dict):
        return Jsonb(obj)
    else:
        return None


if __name__ == '__main__':
    import random
    async def main():
        async with aiohttp.ClientSession() as sesh:
            fb = FalseBardiya()
            await fb.load_it_async(session=sesh,
                                   item_id=str(random.randint(1000,10000000)),
                                   see_progress=False)
        for a,b in fb.get_all_data().items():
            print(f'{a} :: {b}\n')
    
    asyncio.run(main())