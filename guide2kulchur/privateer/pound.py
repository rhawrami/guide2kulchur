import requests
from bs4 import BeautifulSoup, Tag
from datetime import datetime
import json
import re
import aiohttp
import asyncio
import time
import warnings
from typing import Optional, Dict, List, Union, Any
from types import SimpleNamespace

from recruits import (_AGENTS, _TIMEOUT, _rand_headers, _check_soup, _get_similar_books, 
                      _query_books, _query_books_async, _parse_id, _get_similar_books_async, _get_script_el)

'''
Pound: the class for collecting GoodReads author data.
'''

class Pound:
    '''"Treat the bureaucrat with every consideration, and when he ultimately dies do not replace him."'''
    def __init__(self):
        '''GoodReads AUTHOR data scraper. Sequential and asynchronous capabilities available.'''
        self._soup: Optional[BeautifulSoup] = None
        self._info_main: Optional[Tag] = None

        self._a_url:  Optional[str] = None
    
    async def load_author_async(self,
                                session: aiohttp.ClientSession,
                                author_identifier: Optional[str] = None) -> Optional['Pound']:
        '''
        load GoodReads author data asynchronously.

        :param session:
         an aiohttp.ClientSession object
        :param author_identifier (str):
         url to GoodReads author page.'''
        try:
            if author_identifier:
                if len(re.compile(r'^https://www.goodreads.com/author/show/\d*').findall(author_identifier)) > 0:
                    author_identifier = author_identifier
                elif len(re.compile(r'^\d*$').findall(author_identifier)) > 0:
                    author_identifier = f'https://www.goodreads.com/author/show/{author_identifier}'
                else:
                    raise ValueError('author_identifier must be full URL string OR identification serial number')
            else:
                raise ValueError('Give me an identifier damnit!')
            self.a_url = author_identifier
            print(f'attempting {self.a_url} at time {time.ctime()}')
            async with session.get(url=self.a_url,
                                   headers=_rand_headers(_AGENTS)) as resp:
                
                if resp.status != 200:
                    print(f'{resp.status} for {self.a_url}')
                    return None
                
                text = await resp.text()
                soup = BeautifulSoup(text,'lxml')
                info_main = soup.find('div', class_='mainContentFloat')
                info_left = info_main.find('div', class_='leftContainer authorLeftContainer')
                info_right = info_main.find('div', class_='rightContainer')
                
                self._soup = soup
                self._info_main = info_main
                self._info_left = info_left
                self._info_right = info_right
                
                print(f'{self.a_url} pulled at time {time.ctime()}')
                return self
            
        except asyncio.TimeoutError:
            print(f"Timeout loading {self.a_url}")
            return None
        except aiohttp.ClientError as er:
            print(f"Client error loading {self.a_url}: {er}")
            return None
        except Exception as er:
            print(f"Error loading book {author_identifier}: {er}")
    
    def load_author(self,
                  author_identifier: Optional[str] = None) -> Optional['Pound']:
        '''
        load GoodReads author data.

        :param session:
         an aiohttp.ClientSession object
        '''
        try:
            if author_identifier:
                if len(re.compile(r'^https://www.goodreads.com/author/show/\d*').findall(author_identifier)) > 0:
                    author_identifier = author_identifier
                elif len(re.compile(r'^\d*$').findall(author_identifier)) > 0:
                    author_identifier = f'https://www.goodreads.com/author/show/{author_identifier}'
                else:
                    raise ValueError('author_identifier must be full URL string OR identification serial number')
            else:
                raise ValueError('Give me an identifier damnit!')
            self.a_url = author_identifier

            resp = requests.get(self.a_url,headers=_rand_headers(_AGENTS))
            text = resp.text
            soup = BeautifulSoup(text,'lxml')
            info_main = soup.find('div', class_='mainContentFloat')
            info_left = info_main.find('div', class_='leftContainer authorLeftContainer')
            info_right = info_main.find('div', class_='rightContainer')
            
            self._soup = soup
            self._info_main = info_main
            self._info_left = info_left
            self._info_right = info_right
            
            return self
        
        except requests.HTTPError as er:
            print(er)
            return None
    
    def get_name(self) -> Optional[str]:
        '''returns author name'''
        h1 = self._info_right.find('h1', class_='authorName')
        if h1:
            name = h1.find('span')
            return name.text.strip() if name else None
        else:
            return None
    
    def get_id(self) -> Optional[str]:
        '''returns author ID.'''
        return _parse_id(self.a_url)
    
    def get_image_path(self) -> Optional[str]:
        '''returns image path of author'''
        try:
            img = self._info_left.find('img')
            return img['src'].strip()
        except Exception:
            return None
    
    def get_birth_date(self) -> Optional[str]:
        '''returns birth date'''
        bd = self._info_right.find('div', {'itemprop': 'birthDate'})
        if bd:
            bdt = bd.text.strip()
            birth_date = datetime.strptime(bdt,'%B %d, %Y').strftime('%m/%d/%Y')
            return birth_date
        else:
            return None
    
    def get_death_date(self) -> Optional[str]:
        '''returns death date'''
        dd = self._info_right.find('div', {'itemprop': 'deathDate'})
        if dd:
            ddt = dd.text.strip()
            death_date = datetime.strptime(ddt,'%B %d, %Y').strftime('%m/%d/%Y')
            return death_date
        else:
            return None
        
    def get_top_genres(self) -> Optional[List[str]]:
        '''returns top genres author's top genres'''
        try:
            genre_title = [i for i in self._info_right.find_all('div', class_='dataTitle') if i.text == 'Genre'][0]
            genre_box = genre_title.find_next_siblings()[0]
            genres = []
            for genre in genre_box.find_all('a'):
                genres.append(genre.text.strip())
            return genres
        except Exception:
            return None
    
    def get_influences(self) -> Optional[List[Dict[str,str]]]:
        '''returns other writers that author was influenced by'''
        try:
            data_titles = self._info_right.find_all('div', class_ = 'dataTitle')
            influence_txt = [dt for dt in data_titles if 'fluence' in dt.text][0]
            if 'fluence' in influence_txt.text:
                influence_box = influence_txt.find_next_sibling('div', class_ = 'dataItem').find_all('span')[-1]
                influences = []
                for author in influence_box.find_all('a'):
                    name = author.text.strip()
                    id_ = _parse_id(author['href'].strip())
                    influences.append(
                        {'author': name, 'id': id_}
                    )
                return influences if len(influences) > 0 else None
            else:
                return None
        except Exception:
            return None
    
    def get_description(self) -> Optional[str]:
        '''returns author's description'''
        try:
            author_info = self._info_right.find('div', class_ = 'aboutAuthorInfo')
            info = author_info.find_all('span')[-1]
            return info.text.strip()
        except Exception:
            return None
    
    def get_follower_count(self) -> Optional[int]:
        '''returns number of users following the author'''
        try:
            h2 = self._info_left.find_all('h2')
            followers = [h.text.strip() for h in h2 if 'follower' in h.text.strip().lower()][0]
            follow_count_str = re.search(r'\(\d*.*\)',followers).group(0)
            follow_count = re.sub(r'\(|\)|,','',follow_count_str)
            return int(follow_count)
        except:
            return None
            
    def get_num_ratings(self) -> Optional[int]:
        '''returns number of ratings given to author's works'''
        try:
            agg_stats = self._info_right.find('div', class_ = 'hreview-aggregate')
            num_rate_str = agg_stats.find('span', {'itemprop': 'ratingCount'}).text.strip()
            num_rate = num_rate_str.replace(',','')
            return int(num_rate)
        except Exception:
            return None
    
    def get_num_reviews(self) -> Optional[int]:
        '''returns number of reviews given to author's works'''
        try:
            agg_stats = self._info_right.find('div', class_ = 'hreview-aggregate')
            num_rev_str = agg_stats.find('span', {'itemprop': 'reviewCount'}).text.strip()
            num_rev = num_rev_str.replace(',','')
            return int(num_rev)
        except Exception:
            return None
    
    def get_average_rating(self) -> Optional[float]:
        '''returns author's average rating'''
        try:
            agg_stats = self._info_right.find('div', class_ = 'hreview-aggregate')
            avg_rate = agg_stats.find('span', {'itemprop': 'ratingValue'}).text.strip()
            return float(avg_rate)
        except Exception:
            return None
    
    def get_sample_books(self) -> Optional[List[Dict[str,Any]]]:
        '''returns sample (max n = 10) of author's most popular books'''
        # if anyone ever reads this: I know, this is very ugly. 
        # But it works most of the time probably. I haven't written any tests yet.
        try:
            agg_stats = self._info_right.find('div', class_ = 'hreview-aggregate')
            books_tab = agg_stats.find_next_sibling('table').find_all('tr', {'itemtype': 'http://schema.org/Book'})
            
            books = []
            for bk in books_tab:
                elements = bk.find_all('td')
                bk_info = [el for el in elements if el.find('a', class_ = 'bookTitle')][0]
                bkt = bk_info.find('a', class_ = 'bookTitle')
                bkstats = [sp for sp in bk_info.find_all('span') 
                        if 'edition' in sp.text
                        or 'publish' in sp.text][0].text.strip() # what a mess lol
                
                try:
                    pub = re.search(r'published\n.*\d*',bkstats) ## WHAT IF THERE's NO A TAG
                    if pub:
                        bk_yr_pub = re.sub(r'\s|[a-zA-Z]','',pub.group(0))
                        bk_yr_pub = int(bk_yr_pub)
                    else:
                        bk_yr_pub = None
                    
                    avgrat = re.search(r'.*avg rating',bkstats)
                    if avgrat:
                        bk_avg_rat = re.sub(r'\s|[a-zA-Z]','',avgrat.group(0))
                        bk_avg_rat = float(bk_avg_rat)
                    else:
                        bk_avg_rat = None
                    
                    numrat = bkstats.split('—')
                    if len(numrat) > 1:
                        bk_num_rat = re.sub(r'\s|[a-zA-Z]|,','',numrat[1])
                        bk_num_rat = int(bk_num_rat)
                    else:
                        bk_num_rat = None

                    bk_title = bkt.text.strip()
                    bk_id = _parse_id(bkt['href'].strip())

                    bk_dict = {
                        'name': bk_title,
                        'id': bk_id,
                        'year_published': bk_yr_pub,
                        'rating_average': bk_avg_rat,
                        'rating_count': bk_num_rat
                    }
                    books.append(bk_dict)
                except Exception:
                    continue
            return books
        
        except Exception:
            return None

    

if __name__=='__main__':
    async def main():
        async with aiohttp.ClientSession(headers=_rand_headers(_AGENTS)) as session:
            pnd = Pound()
            usr = await pnd.load_author_async(session=session,
                                            author_identifier='35258')
            st = f'''
NAME: {usr.get_name()}

ID: {usr.get_id()}

BIRTH: {usr.get_birth_date()}

DEATH: {usr.get_death_date()}

TOP GENRES: {usr.get_top_genres()}

DESCRIPTION: {usr.get_description()}

AVG RATING: {usr.get_average_rating()}

NUM RATING: {usr.get_num_ratings()}

NUM REVIEW: {usr.get_num_reviews()}

FOLLOWER COUNT: {usr.get_follower_count()}

IMG PATH: {usr.get_image_path()}

INFLUENCES: {usr.get_influences()}

SAMPLE OF BOOKS
                  '''
            print(st)
            for bk in usr.get_sample_books():
                for a,b in bk.items():
                    print(f'{a} :: {b}')
                print()

    asyncio.run(main())