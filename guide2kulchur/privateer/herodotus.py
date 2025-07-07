import requests
from bs4 import BeautifulSoup
import json
import re
import aiohttp
import asyncio
import random

from recruits import AGENTS, TIMEOUT, rand_headers, _parse_id
from alexandria import Alexandria

'''
Herodotus, the OG historian and arguably the most prescient 
man in history, has been vindicated so many times that MLE should
be renamed to Herodotilian estimation. But back to scraping. In 
herodotus.py, we'll be scraping time trends.

Given that we're just scraping from the annual awards, this won't 
be as optimized as Alexandria, but it'll be fine for our purposes
'''

def _get_num_votes(element):
    '''returns number of votes for a book'''
    try:
        el_votes = element.find('strong',class_='uitext result').text.strip().lower()
        num_votes = int(re.sub(r',|\svotes$','',el_votes))
        return num_votes
    except Exception as er:
        print(er)

def _get_bk_url_and_id(element):
    '''returns url and id of a book in tuple form (url,id)'''
    try:
        rel_url = element.find('a',class_='pollAnswer__bookLink')['href']
        gbk_url = 'https://www.goodreads.com' + rel_url
        gbk_id = _parse_id(gbk_url)
        return (gbk_url,gbk_id)
    except Exception as er:
        print(er)

    
YR_START = 2011
YR_END = 2024
URL_ = 'https://www.goodreads.com/choiceawards/best-books-{yr}'

async def _get_books_from_genre(session: aiohttp.ClientSession,
                                genre_url: str,
                                cat: str,
                                year: int)->list:
    try:
        async with session.get(url=genre_url,headers=rand_headers(AGENTS),timeout=TIMEOUT) as resp:
            if resp.status != 200:
                    print(f'{resp.status} for {genre_url}')
                    return None
            
            text = await resp.text()
            soup = BeautifulSoup(text,'lxml')

            pollcontents = soup.find('div',class_='pollContents')
            if pollcontents:
                g_bks = pollcontents.find_all('div',class_='inlineblock pollAnswer resultShown')
                if g_bks:
                    g_bks_metadat = {
                        _get_bk_url_and_id(gbk)[1]: {
                            'num_votes': _get_num_votes(gbk),
                            'won_url': _get_bk_url_and_id(gbk)[0],
                            'genre_won': cat,
                            'year_won': year
                        } for gbk in g_bks
                    }

                    g_bks_dat = await Alexandria.multiload_books(books=[gbk for gbk in g_bks_metadat.keys()],
                                                                 write_json=False,
                                                                 max_concurrent=3)
                    new_dat = []
                    for bk in g_bks_dat:
                        id_ = bk['id']
                        new_dict = dict(**g_bks_metadat[id_], **bk)
                        new_dat.append(new_dict)
                    
                    return new_dat

    except asyncio.TimeoutError:
            print(f"Timeout loading {genre_url}")
            return None
    except aiohttp.ClientError as er:
        print(f"Client error genre {genre_url}: {er}")
        return None
    except Exception as er:
        print(f"Error loading genre {genre_url}: {er}")

class Herodotus:
    '''The Ionians are calling, brother. Will you pick up?'''
    def __init__(self):
        '''scrape Anual GoodReads Choice Awards data.'''
    
    async def scrape1yr(self,
                        session: aiohttp.ClientSession,
                        year: int)->list:
        '''scrapes one year of Annual GoodReads Choice Awards school data
        
        :param year: (int) awards year, ex. yr = 2022
        :param return_dat: (bool) if True, returns data in dict format
        '''
        year_url = URL_.format(yr=year)
        try:
            async with session.get(url=year_url,
                                   headers=rand_headers(AGENTS)) as resp:
                if resp.status != 200:
                    print(f'{resp.status} for {year_url}')
                    return None
                
                text = await resp.text()
                soup = BeautifulSoup(text,'lxml')

                container1 = soup.find('div',class_='clearFix',id='categories')
                if container1:
                    container2 = container1.find_all('div',class_='category clearFix')
                    if container2:

                        cat_urls = [
                            (f"https://www.goodreads.com{i.find('a')['href']}", # Category url, e.g., https://www.goodreads.com/choiceawards/readers-favorite-fiction-books-2024
                            i.find('a').find('h4').text.strip()) # Category title, e.g., Fiction
                            for i
                            in container2
                        ]

                dat = []
                failed_cats = []
                for cat_url,cat in cat_urls:
                    try:
                        res = await _get_books_from_genre(session,cat_url,cat,year)
                        dat.append(res)
                    except Exception as er:
                        print(f'Error for {cat}: {er}')
                        failed_cats.append((cat_url,cat))
                    finally:
                        await asyncio.sleep(5)
                return dat, failed_cats
    
        except asyncio.TimeoutError:
            print(f"Timeout loading {year}")
            return None
        except aiohttp.ClientError as er:
            print(f"Client error loading {year}: {er}")
            return None
        except Exception as er:
            print(f"Error loading year {year}: {er}")
    
    @staticmethod
    async def scrape_multiyear(years=[],
                               max_concurrent = 3,
                               write_json=True,
                               json_path=''):
        '''downloads multiple years of Goodreads Awards data'''


if __name__=='__main__':
    async def main():
        connector = aiohttp.TCPConnector(
                                limit=100,
                                limit_per_host=20,
                                ttl_dns_cache=300,
                                use_dns_cache=True
                                )
        async with aiohttp.ClientSession(connector=connector) as session:
            hrd = Herodotus()
            dat_23 = await hrd.scrape1yr(session=session, year=2023)
            return dat_23
    
    books = asyncio.run(main())
    
    
    
