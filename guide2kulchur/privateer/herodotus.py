import requests
from bs4 import BeautifulSoup
import json
import re
import aiohttp
import asyncio
import random

from recruits import AGENTS, TIMEOUT, rand_headers
from alexandria import Alexandria

'''
Herodotus, the OG historian and arguably the most prescient 
man in history, has been vindicated so many times that MLE should
be renamed to Herodotilian estimation. But back to scraping. In 
herodotus.py, we'll be scraping time trends.

Given that we're just scraping from the annual awards, this won't 
be as optimized as Alexandria, but it'll be fine for our purposes
'''



YR_START = 2011
YR_END = 2024
URL_ = 'https://www.goodreads.com/choiceawards/best-books-{yr}'

async def _get_books_from_genre(session: aiohttp.ClientSession,
                                genre_url: str,
                                cat: str)->list:
    final_dat = []
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
                    for gbk in g_bks:
                        num_votes = gbk.find('strong',class_='uitext result').text.strip().lower()
                        num_votes = int(re.sub(r',|\svotes$','',num_votes))
                        gbk_url = 'https://www.goodreads.com' + gbk.find('a',class_='pollAnswer__bookLink')['href']
                        gbk_url = re.sub(r'\?from_choice=true','',gbk_url)

                        alx = Alexandria()
                        await alx.load_book_async(session,gbk_url)
                        gbk_dict = alx.get_all_data()
                        
                        gbk_dict['win_category'] = cat
                        gbk_dict['win_num_votes'] = num_votes
                        final_dat.append(gbk_dict)

                        await asyncio.sleep(5)
                    return final_dat
                else:
                    return None
            

    except asyncio.TimeoutError:
            print(f"Timeout loading {genre_url}")
            return None
    except aiohttp.ClientError as er:
        print(f"Client error loading {genre_url}: {er}")
        return None
    except Exception as er:
        print(f"Error loading book {genre_url}: {er}")

class Herodotus:
    '''The Ionians are calling, brother. Will you pick up?'''
    def __init__(self):
        '''scrape Anual GoodReads Choice Awards data.'''
        self.dat = {str(i):[] for i in range(YR_START,YR_END+1)}
    
    async def scrape1yr_async(self,
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
                            (f'https://www.goodreads.com{i.find('a')['href']}', # Category url, e.g., https://www.goodreads.com/choiceawards/readers-favorite-fiction-books-2024
                            i.find('a').find('h4').text.strip()) # Category title, e.g., Fiction
                            for i
                            in container2
                        ]

                tot_dat = []
                tasks = [
                    _get_books_from_genre(session,cat_url,cat) for cat_url,cat in cat_urls
                ]

                res = await asyncio.gather(*tasks,return_exceptions=True)
                for g in res:
                    tot_dat.extend(g)
                return tot_dat
                # for cat_url, cat in cat_urls:
                #     cat_dat = await _get_books_from_genre(session,cat_url,cat)
                #     if cat_dat:
                #         tot_dat.extend(cat_dat)
                #         await asyncio.sleep(random.randint(0,1))
                #     else:
                #         return None
                # return tot_dat    
            
        except asyncio.TimeoutError:
            print(f"Timeout loading {year}")
            return None
        except aiohttp.ClientError as er:
            print(f"Client error loading {year}: {er}")
            return None
        except Exception as er:
            print(f"Error loading book {year}: {er}")

    # def scrape1yr(self,
    #               year: int,
    #               return_dat: bool):
    #     yr_url = URL_.format(yr=year)
    #     dat = []
    #     try:
    #         r = requests.get(url=yr_url,headers=rand_headers(AGENTS))
    #         soup = BeautifulSoup(r.text,'lxml')
    #         container1 = soup.find('div',class_='clearFix',id='categories')
    #         if container1:
    #             container2 = container1.find_all('div',class_='category clearFix')
    #             if container2:
    #                 cat_urls = [
    #                     (f'https://www.goodreads.com{i.find('a')['href']}', # Category url, e.g., https://www.goodreads.com/choiceawards/readers-favorite-fiction-books-2024
    #                     i.find('a').find('h4').text.strip()) # Category title, e.g., Fiction
    #                     for i
    #                     in container2
    #                 ]

    #         container1 = soup.find('div',class_='clearFix',id='categories')
    #         if container1:
    #             container2 = container1.find_all('div',class_='category clearFix')
    #             if container2:
    #                 cat_urls = [
    #                     (f'https://www.goodreads.com{i.find('a')['href']}', # Category url, e.g., https://www.goodreads.com/choiceawards/readers-favorite-fiction-books-2024
    #                     i.find('a').find('h4').text.strip()) # Category title, e.g., Fiction
    #                     for i
    #                     in container2
    #                 ]

    #         for c_url,cat in cat_urls:
    #             try:
    #                 r2 = requests.get(c_url,headers=rand_headers())
    #                 soup2 = BeautifulSoup(r2.text,'lxml')
    #                 pollcontents = soup2.find('div',class_='pollContents')
    #                 for bk in pollcontents.find_all('div',class_='inlineblock pollAnswer resultShown'):
    #                     num_votes = bk.find('strong',class_='uitext result').text.strip().lower()
    #                     num_votes = int(re.sub(r',|\svotes$','',num_votes))
    #                     bk_url = 'https://www.goodreads.com' + bk.find('a',class_='pollAnswer__bookLink')['href']

    #                     bk_dict = Alexandria().load_book(book_identifier=bk_url).get_all_data()
    #                     bk_dict['win_category'] = cat
    #                     bk_dict['win_num_votes'] = num_votes
    #                     print(bk_dict['title'],bk_dict['first_published'],bk_dict['rating'])
    #                     print()
    #                     dat.append(bk_dict)
                    
    #                 self.dat[str(year)] = dat
    #                 if return_dat:
    #                     return dat
                    
    #             except requests.HTTPError as er:
    #                 print(er)
                
    #     except requests.HTTPError as er:
    #         print(er)


        


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
            dat_23 = await hrd.scrape1yr_async(session=session, year=2015)
            return dat_23
    
    books = asyncio.run(main())
    
    
    
