import time
import asyncio
import re
import json
from typing import (
    List, 
    Dict, 
    Any, 
    Optional, 
    Union, 
    Tuple,
)

import aiohttp
from bs4 import BeautifulSoup

from guide2kulchur.privateer.recruits import (
    _TIMEOUT, 
    _rand_headers, 
    _AGENTS,
)


class Plato:
    '''Plato: collect PUBLICLY AVAILABLE genre urls; meant to be start of pipeline.'''
    def __init__(self):
        '''Goodreads genre data url collector. One time use.'''
        pass
    
    async def _load_one_genre_page(self,
                                   session: aiohttp.ClientSession,
                                   page_number: int,
                                   semaphore: asyncio.Semaphore,
                                   num_attempts: int = 3,
                                   see_progress: bool = True) -> Optional[Union[List[Dict], Tuple[int,List[Dict]]]]:
        '''
        Collect one Goodreads genre page full of URLs.

        :param session: an aiohttp ClientSession
        :param page_number: page number of genre list
        :param semaphore: Semaphore object
        :num_attempts: number of attempts to successfully request a genre list page
        :see_progress: if True, prints progress statements, like success and retry messages
        '''
        async with semaphore:
            page_url = f'https://www.goodreads.com/genres/list?page={page_number}'
            print(f'attempt genre page {page_number} :: {time.ctime()}') if see_progress else None
            for attempt in range(num_attempts):
                try:
                    async with session.get(url=page_url,
                                           headers=_rand_headers(_AGENTS)) as resp:
                        if resp.status != 200:
                            print(f'Improper code ({resp.status}) for  {page_number} :: {time.ctime()}') if see_progress else None
                            return None
                        
                        text = await resp.text()
                        soup = BeautifulSoup(text,'lxml')
                        genres = soup.find_all('div', class_ = 'shelfStat')
                        
                        genres_dat = []
                        for genre in genres:
                            try:
                                genre_name = genre.find('a').text.strip()
                                genre_url = 'https://www.goodreads.com' + genre.find('a')['href'].strip()
                                genre_bk_size_str = genre.find('div', class_ = 'smallText').text.strip()
                                genre_bk_size_str = re.sub(r' books|,','',genre_bk_size_str)
                                genre_bk_size = int(genre_bk_size_str)
                                genre_d = {
                                    'url': genre_url,
                                    'name': genre_name,
                                    'size': genre_bk_size
                                }
                                genres_dat.append(genre_d)
                            except Exception:
                                continue
                        print(f'pulled genre page {page_number} :: {time.ctime()}')
                        
                        if page_number == 1:    # this is meant to be dynamic for future retries, but is pretty sketchy rn
                            observed_limit = 0
                            KNOWN_LIMIT = 15
                            all_As = soup.find_all('a')
                            if all_As:
                                final_A = all_As[-1]
                                try:
                                    p_num = re.search(r'^[0-9]$', final_A.text)
                                    p_num = int(p_num)
                                    observed_limit = p_num
                                except Exception:
                                    observed_limit = KNOWN_LIMIT
                            else:
                                observed_limit = KNOWN_LIMIT
                            return (observed_limit, genres_dat)

                    return genres_dat
                
                except asyncio.TimeoutError:
                    SLEEP_SCALAR = 1.5
                    sleep_time = (attempt + 1) ** SLEEP_SCALAR
                    await asyncio.sleep(sleep_time)
                    print(f'retrying genre page {page_number} :: {time.ctime()}') if see_progress else None

                except Exception as er:
                    print(f'fatal error ({er}) for genre page {page_number} :: {time.ctime()}') if see_progress else None
                    return None


    async def get_genre_urls(self,
                             semaphore_count: int = 3,
                             num_attempts: int = 3,
                             batch_delay: Optional[int] = 1,
                             batch_size: Optional[int] = 5,
                             see_progress: bool = True,
                             write_json: Optional[str] = None) -> List[Dict[str,Any]]:
        '''Collect list of Goodreads genres and URLs.
        
        :param semaphore_count: number of "concurrent" genre page requests
        :param num_attempts: number of attempts to request each page
        :param batch_delay: sets delay between each batch of genre page request
        :param batch_size: sets batch size
        :param see_progress: if True, prints progress statements
        :param write_json: if True, writes genre URLs to JSON file
        '''
        sem = asyncio.Semaphore(semaphore_count)
        tot_dat = []
        ctr = 0
        successes = 0
        
        t_start = time.ctime()
        async with aiohttp.ClientSession(timeout=_TIMEOUT,
                                         headers=_rand_headers(_AGENTS)) as sesh:
            res_1 = await self._load_one_genre_page(session=sesh,
                                                    page_number=1,
                                                    semaphore=sem,
                                                    num_attempts=num_attempts,
                                                    see_progress=see_progress)
            ctr += 1
            if not res_1:
                raise RuntimeError('Fatal Error on page 1.')
            res_1_dat = res_1[1]
            tot_dat.extend(res_1_dat)
            successes += 1
            
            num_pages = res_1[0] 
            other_tasks = [self._load_one_genre_page(session=sesh,
                                                     page_number=i,
                                                     semaphore=sem,
                                                     num_attempts=num_attempts,
                                                     see_progress=see_progress) for i in range(2,num_pages+1)]
            
            async for task in asyncio.as_completed(other_tasks):
                if batch_delay and batch_size:
                    if ctr > 0 and ctr % batch_size == 0:
                        time.sleep(batch_delay)

                res = await task
                if res:
                    tot_dat.extend(res)
                    successes += 1
                ctr += 1
            t_end = time.ctime()
        
        attempted = ctr
        failures = attempted - successes
        success_rate = successes  / attempted
        if write_json:
            json_dat = {
                'started_at': t_start,
                'ended_at': t_end,
                'attempts': attempted,
                'successes': successes,
                'failures': failures,
                'success_rate': success_rate,
                'number_genres': len(tot_dat),
                'results': tot_dat
            }
            with open(write_json,'w') as json_file:
                json.dump(json_dat,json_file,indent=4)
        
        metadat = f'''
------------------------------------
started at: {t_start}
ended at: {t_end}
attempted: {attempted}
successes: {successes}
failures: {failures}
success rate: {success_rate}
------------------------------------
'''
        print(metadat)

        return tot_dat
    



