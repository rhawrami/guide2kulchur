import time
import asyncio
from typing import List, Optional

import aiohttp
from bs4 import BeautifulSoup

from guide2kulchur.privateer.recruits import (
    _TIMEOUT, 
    _rand_headers, 
    _AGENTS, 
    _parse_id
)


async def _req_genre_page(session: aiohttp.ClientSession,
                          genre_name: str,
                          most_read_or_shelf: str,
                          num_attempts: int = 3,
                          see_progress: bool = True) -> Optional[BeautifulSoup]:
    '''
    try a genre request, either for most read books, or the top shelf.

    :param session: an aiohttp ClientSession
    :param genre_name: a Goodreads genre name
    :param most_read_or_shelf: either "most_read" or "shelf"
    :param num_attempts: number of attempts for each genre page
    :param see_progress: if True, prints progress statements
    '''
    if most_read_or_shelf == 'most_read':
        req_url = f'https://www.goodreads.com/genres/most_read/{genre_name}'
    elif most_read_or_shelf == 'shelf':
        req_url = f'https://www.goodreads.com/shelf/show/{genre_name}'
    else:
        return None
    
    print(f'attempt :: {most_read_or_shelf} ({genre_name}) :: {time.ctime()}')
    for attempt in range(num_attempts):
        try:
            async with session.get(url=req_url,
                                    headers=_rand_headers(_AGENTS),
                                    timeout=_TIMEOUT) as resp:
                if resp.status != 200:
                    print(f'{resp.status} for {genre_name} :: {time.ctime()}') if see_progress else None
                    return None
                
                text = await resp.text()
                soup = BeautifulSoup(text,'lxml')
                print(f'pulled :: {most_read_or_shelf} ({genre_name}) :: {time.ctime()}')
                return soup
                            
        except asyncio.TimeoutError:
            SLEEP_SCALAR = 1.5
            sleep_time = (attempt + 1) ** SLEEP_SCALAR
            await asyncio.sleep(sleep_time)
            print(f'retrying :: {most_read_or_shelf} {genre_name} :: {time.ctime()}') if see_progress else None
        
        except Exception as er:
            print(f'unexpected error ({er}) :: {most_read_or_shelf} {genre_name} :: {time.ctime()}') if see_progress else None
            return None
    return None

    
class Plotinus:
    '''Plotinus: collect PUBLICLY AVAILABLE genre data.'''
    def __init__(self):
        '''Goodreads genre data collector.'''
        pass
    

    async def get_most_read_this_week(self,
                                      session: aiohttp.ClientSession,
                                      genre_name: str,
                                      num_attempts: int = 3,
                                      see_progress: bool = True) -> Optional[List[str]]:
        '''
        loads IDs for Goodreads books most read this week for a given genre.
        
        :param session: an aiohttp ClientSession
        :param genre_name: a Goodreads genre name
        :param num_attempts: number of attempts for each genre page
        :param see_progress: if True, prints progress statements
        '''
        soup = await _req_genre_page(session=session,
                                     genre_name=genre_name,
                                     most_read_or_shelf='most_read',
                                     num_attempts=num_attempts,
                                     see_progress=see_progress)
        if not soup:
            return None            
        
        if not (cover_rows := soup.find_all('div', class_ = 'coverRow')):   # never used walrus operator, but I like how it looks :=
            cover_rows = soup.find_all('div', class_ = 'coverRow   ')   
            if not cover_rows:
                print(f'No book rows found for {genre_name} :: {time.ctime()}') if see_progress else None
                return None
        
        book_ids = []
        for row in cover_rows:
            bks = row.find_all('div', class_ = ['leftAlignedImage', 'bookBox'])
            if bks:
                for bk in bks:
                    if (a_tag := bk.find('a')):
                            if a_tag.get('href') and (bk_id := _parse_id(url=a_tag['href'])):
                                book_ids.append(bk_id)
        
        return book_ids if book_ids else None


    async def get_top_shelf(self,
                            session: aiohttp.ClientSession,
                            genre_name: str,
                            num_attempts: int = 3,
                            see_progress: bool = True) -> List[str]:
        '''
        loads IDs for the top Goodreads books for a given genre.

        :param session: an aiohttp ClientSession
        :param genre_name: a Goodreads genre name
        :param num_attempts: number of attempts for each genre page
        :param see_progress: if True, prints progress statements
        '''
        soup = await _req_genre_page(session=session,
                                     genre_name=genre_name,
                                     most_read_or_shelf='shelf',
                                     num_attempts=num_attempts,
                                     see_progress=see_progress)
        if not soup:
            return None            
        if not (container := soup.find('div', class_ = 'leftContainer')):
            return None
        
        bk_elements = container.find_all('div', class_ = 'elementList')
        book_ids = []
        for bk in bk_elements:
            if (title := bk.find('a', class_ = 'bookTitle')) and title.get('href'):
                if bk_id := _parse_id(title.get('href')):
                    book_ids.append(bk_id)
        
        return book_ids if book_ids else None
        





                        
                
