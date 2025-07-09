import asyncio
import re
from typing import Optional, Dict, List, Tuple, Any
import random

import aiohttp
from bs4 import BeautifulSoup

from guide2kulchur.privateer.recruits import _AGENTS, _TIMEOUT, _rand_headers, _parse_id
from guide2kulchur.privateer.alexandria import Alexandria

_awards_url = 'https://www.goodreads.com/choiceawards/best-books-{year}'

class Herodotus:
    '''"For God tolerates pride in none but Himself. Haste is the mother of failure - 
       and for failure we always pay a heavy price; it is in delay our profit lies - 
       perhaps it may not immediately be apparent, but we shall find it, sure enough, as time goes on."'''
    def __init__(self, 
                 semaphore: int):
        '''Scrape Goodreads Annual Choice Awards book data asynchronously.
        
        :semaphore: value fed into an asyncio Semaphore, controlling number of requests in this case.
        '''
        self._sem = asyncio.Semaphore(semaphore)
    

    async def _pull_categories(self,
                               session: aiohttp.ClientSession,
                               year: int) -> List[Tuple[str,str]]:
        '''returns urls to categoried awards for a given year
        
        :session: an aiohttp ClientSession
        :year: a given award year
        '''
        y_url = _awards_url.format(year=year)

        try:
            async with session.get(url = y_url, 
                               timeout=_TIMEOUT,
                               headers=_rand_headers(_AGENTS)) as resp:
                if resp.status != 200:
                    print(f'{resp.status} for {year}')
                    return None
                    
                text = await resp.text()
                
        except asyncio.TimeoutError:
            print(f"Timeout loading {year}")
            return None
        except aiohttp.ClientError as er:
            print(f"Client error loading {year}: {er}")
            return None
        except Exception as er:
            print(f'Other error loading {year}: {er}')
        
        soup = BeautifulSoup(text,'lxml')
        cat_box = soup.find('div', class_ = 'categoryContainer')

        cats = []
        for cat in cat_box.find_all('div', class_ = ['category','clearFix']):
            try:
                c_partial_url = cat.find('a')['href'].strip()
                c_url = 'http://goodreads.com' + c_partial_url
                c_desc = cat.find('h4', class_ = 'category__copy').text.strip()
                cats.append((c_desc,c_url)) 
            except TypeError:
                continue
        
        return cats
    
    
    async def _pull_single_bk_dat(self,
                                  session: aiohttp.ClientSession,
                                  year: int,
                                  category: str,
                                  bk_id: str) -> Optional[Alexandria]:
        '''pulls data for a single book, using Alexandria
        
        :session: an aiohttp ClientSession
        :year: award year
        :category: award category
        :bk_id: Goodreads book ID
        '''
        try:
            async with self._sem:
                alx = Alexandria()
                await alx.load_book_async(session,
                                    bk_id)
                dat = await alx.get_all_data_async(session=session,
                                                   exclude_attrs=['similar_books'],
                                                   to_dict= True)
                print(f'{dat['title']} by {dat['author']} @ [{category}] ({year})')
                
            await asyncio.sleep(random.uniform(0,1))

        except Exception as er:
            print(er)
            return None
        
        return dat
    

    async def _pull_bk_data(self,
                            session: aiohttp.ClientSession,
                            year: int,
                            category_desc: str,
                            category_url: str) -> List[Dict[str,Any]]:
        '''pulls total book data from a given award category in a given year.
        
        :session: an aiohttp ClientSession
        :year: award year to pull data from
        :category_desc: award category description
        :category_url: award category URL
        '''
        try:
            async with self._sem:
                async with session.get(url = category_url,
                                timeout=_TIMEOUT,
                                headers=_rand_headers(_AGENTS)) as resp:
                
                    if resp.status != 200:
                            print(f'{resp.status} for {category_url}')
                            return None
                        
                    text = await resp.text()
                
                soup = BeautifulSoup(text,'lxml')
            poll_box = soup.find('div', class_ = 'pollContents')
            
            bk_dat = []
            for bk in poll_box.find_all('div', class_ = ['inlineblock', 'pollAnswer']):
                try:
                    num_votes_str = bk.find('strong').text.strip()
                    num_votes_cln = re.sub(r',|\svotes|\s','',num_votes_str)
                    num_votes = int(num_votes_cln)
                    
                    bk_url = 'http://goodreads.com' + bk.find('a', class_ = 'pollAnswer__bookLink')['href'].strip()
                    bk_id = _parse_id(bk_url)
                    bk_d = {
                        'year': year,
                        'category': category_desc,
                        'id': bk_id,
                        'num_votes': num_votes
                    }
                    bk_dat.append(bk_d)
                    
                except TypeError:
                    continue

            bk_ids = [bk['id'] for bk in bk_dat]
            tasks = [self._pull_single_bk_dat(session=session,
                                              year=year,
                                              category=category_desc,
                                              bk_id=id_) for id_ in bk_ids]

            bkres = await asyncio.gather(*tasks,return_exceptions=True)
            successes = [res for res in bkres if res is not None and not isinstance(res,Exception)]

            await asyncio.sleep(random.uniform(0,2))
            
            num_votes_map = {bk['id']: bk['num_votes'] for bk in bk_dat}
            for s in successes:
                s['award_year'] = year
                s['award_category'] = category_desc
                s['award_num_votes'] = num_votes_map[s['id']]
            
            await asyncio.sleep(random.uniform(0,2))
            return successes
                
        except asyncio.TimeoutError:
            print(f"Timeout loading {category_url}")
            return None
        except aiohttp.ClientError as er:
            print(f"Client error loading {category_url}: {er}")
            return None
    

    async def pull_one_year(self,
                            session: aiohttp.ClientSession,
                            year: int) -> List[Dict[str,Any]]:
        '''Pull one year of Goodreads Annual Choice Awards book data asynchronously.
        
        :session: an asyncio ClientSession
        :year: a given year to pull data from
        '''
        try:
            async with self._sem:
                async with session as sesh:
                    cats = await self._pull_categories(sesh,year)
                    
                    master_dat = []
                    for desc,url in cats:
                        bkd = await self._pull_bk_data(session=sesh,
                                                       year=year,
                                                       category_desc=desc,
                                                       category_url=url)                
                        if bkd:
                            master_dat.extend(bkd)
                        
                        await asyncio.sleep(random.uniform(5,10))

        except Exception:
            return None
        
        return master_dat
        

