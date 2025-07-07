import asyncio
import json
import re
import aiohttp
from bs4 import BeautifulSoup
from typing import Optional, Dict, List, Tuple, Any

from recruits import _AGENTS, _TIMEOUT, _rand_headers, _parse_id
from alexandria import Alexandria

_awards_url = 'https://www.goodreads.com/choiceawards/best-books-{year}'
class Herodotus:
    '''collect Goodreads annual book awards data'''
    def __init__(self):
        '''HERODOTUS'''
        self._sem = asyncio.Semaphore(3)
    
    async def _pull_categories(self,
                               session: aiohttp.ClientSession,
                               year: int) -> List[Tuple[str,str]]:
        '''returns urls to categoried awards for a given year'''

        y_url = _awards_url.format(year=year)
        try:
            async with self._sem:
                async with session.get(url = y_url, 
                                timeout=_TIMEOUT,
                                headers=_rand_headers(_AGENTS)) as resp:
                    if resp.status != 200:
                        print(f'{resp.status} for {self.b_url}')
                        return None
                    
                    text = await resp.text()
                
        except asyncio.TimeoutError:
            print(f"Timeout loading {year}")
            return None
        except aiohttp.ClientError as er:
            print(f"Client error loading {year}: {er}")
            return None
        
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
                                  bk_id: str) -> Optional[Alexandria]:
        '''pulls single book data using Alexandria'''

        try:
            async with self._sem:
                async with session as sesh:
                    alx = await Alexandria().load_book_async(sesh,bk_id)
                    await asyncio.sleep(0.5)
                    dat = await alx.get_all_data_async(sesh,
                                                       ['similar_books'],
                                                       to_dict= True)
                    
                    await asyncio.sleep(0.5)

        except Exception as er:
            print(er)
            return None
        
        return dat
    
    async def _pull_bk_data(self,
                            session: aiohttp.ClientSession,
                            year: int,
                            category_desc: str,
                            category_url: str) -> List[Dict[str,Any]]:
        '''pulls book data from a given award category in a given year.'''

        try:
            async with self._sem:
                async with session.get(url = category_url,
                                timeout=_TIMEOUT,
                                headers=_rand_headers(_AGENTS)) as resp:
                
                    if resp.status != 200:
                            print(f'{resp.status} for {self.b_url}')
                            return None
                        
                    text = await resp.text()
                
        except asyncio.TimeoutError:
            print(f"Timeout loading {category_url}")
            return None
        except aiohttp.ClientError as er:
            print(f"Client error loading {category_url}: {er}")
            return None
        
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

        async with session as sesh:
            bk_ids = [bk['id'] for bk in bk_dat]
            tasks = [self._pull_single_bk_dat(sesh,id_) for id_ in bk_ids]

            bkres = await asyncio.gather(*tasks,return_exceptions=True)
            successes = [res for res in bkres if res is not None and not isinstance(res,Exception)]

            await asyncio.sleep(1)
        
        num_votes_map = {bk['id']: bk['num_votes'] for bk in bk_dat}
        for s in successes:
            s['award_year'] = year
            s['award_category'] = category_desc
            s['award_num_votes'] = num_votes_map[s['id']]
        
        await asyncio.sleep(2)
        return successes
    
    async def pull_one_year(self,
                            session: aiohttp.ClientSession,
                            year: int) -> List[Dict[str,Any]]:
        '''returns one year of annual award data'''
        
        try:
            async with session as sesh:
                cats = await self._pull_categories(sesh,year)
                
                master_dat = []
                for desc,url in cats:
                    bkd = await self._pull_bk_data(sesh,year,desc,url)                
                    if bkd:
                        master_dat.extend(bkd)

                await asyncio.sleep(2)

        except Exception:
            return None
        
        return master_dat


if __name__ == '__main__':
    async def main(year: int):
        connector = aiohttp.TCPConnector(
                            limit=20,
                            limit_per_host=5,
                            ttl_dns_cache=300,
                            use_dns_cache=True
                            )
        
        async with aiohttp.ClientSession(timeout=_TIMEOUT,connector=connector) as sesh:
            r = await Herodotus().pull_one_year(sesh,year)
        
        for bk in r:
            print(bk)
    
    asyncio.run(main(2010))
        

