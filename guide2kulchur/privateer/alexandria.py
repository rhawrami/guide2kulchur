import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import re
import aiohttp
import asyncio

from recruits import AGENTS, TIMEOUT, rand_headers, _check_soup, _get_similar_books, _query_books, _query_books_async, _parse_id

'''
Alexandria, the class for collecting GoodReads book data.
'''

class Alexandria:
    '''Alexandria: the reclaimed source of all knowledge.'''
    def __init__(self):
        '''GoodReads single book data scraper. Sequential and asynchronous capabilities available.'''
        self.soup = None
        self.info_main = None
        self.info_main_metadat = None
        self.details = None
        self.b_url = None
    
    async def load_book_async(self,
                              session=aiohttp.ClientSession,
                              book_identifier=None,
                              query_str=None):
        '''
        load GoodReads book data asynchronously.

        :param session:
         an aiohttp.ClientSession object
        :param book_identifier (str):
         url to GoodReads book page.
        :param query_str (str):
         query string to find a book (top result returned).

        Alexandria takes in either the book_identifier arument, with:
        - a full url string to the book; e.g., book_identifier = "https://www.goodreads.com/book/show/7144.Crime_and_Punishment"
        - a unique GoodReads book identifier string; e.g., book_identifier = "7144"

        or the query_str argument, with:
        -  a query string to search for a book (returns top query result); e.g., "Crime and Punishment Dostoevsky"

        Either book_identifier or query_str should be given, not both.'''
        try:
            if book_identifier:
                if len(re.compile(r'^https://www.goodreads.com/book/show/\d*').findall(book_identifier)) > 0:
                    book_identifier = book_identifier
                elif len(re.compile(r'^\d*$').findall(book_identifier)) > 0:
                    book_identifier = f'https://www.goodreads.com/book/show/{book_identifier}'
                else:
                    raise ValueError('book_identifier must be full URL string OR identification serial number')
            elif query_str and not book_identifier:
                book_identifier = await _query_books_async(session,query_str)
            else:
                raise ValueError('Give me a query string or an identifier damnit!')
            self.b_url = book_identifier

            async with session.get(url=self.b_url,
                                   headers=rand_headers(AGENTS)) as resp:
                
                if resp.status != 200:
                    print(f'{resp.status} for {self.b_url}')
                    return None
                
                text = await resp.text()
                soup = BeautifulSoup(text,'lxml')
                info_main = soup.find('div', class_='BookPage__mainContent')
                info_main_metadat = info_main.find('div', class_='BookPageMetadataSection')
                details = info_main_metadat.find('div', class_='FeaturedDetails')
                self.soup = soup
                self.info_main = info_main
                self.info_main_metadat = info_main_metadat
                self.details = details
                print(f'{self.b_url} pulled.')
                return self
            
        except asyncio.TimeoutError:
            print(f"Timeout loading {self.b_url}")
            return None
        except aiohttp.ClientError as er:
            print(f"Client error loading {self.b_url}: {er}")
            return None
        except Exception as er:
            print(f"Error loading book {book_identifier}: {er}")
    
    def load_book(self,
                  book_identifier=None,
                  query_str=None):
        '''
        load GoodReads book data

        :param book_identifier (str):
         url to GoodReads book page.
        :param query_str (str):
         query string to find a book (top result returned).

        Alexandria takes in either the book_identifier arument, with:
        - a full url string to the book; e.g., book_identifier = "https://www.goodreads.com/book/show/7144.Crime_and_Punishment"
        - a unique GoodReads book identifier string; e.g., book_identifier = "7144"

        or the query_str argument, with:
        -  a query string to search for a book (returns top query result); e.g., "Crime and Punishment Dostoevsky"

        Either book_identifier or query_str should be given, not both.
        '''
        try:
            if book_identifier:
                if len(re.compile(r'^https://www.goodreads.com/book/show/\d*').findall(book_identifier)) > 0:
                    book_identifier = book_identifier
                elif len(re.compile(r'^\d*$').findall(book_identifier)) > 0:
                    book_identifier = f'https://www.goodreads.com/book/show/{book_identifier}'
                else:
                    raise ValueError('book_identifier must be full URL string OR identification serial number')
            elif query_str and not book_identifier:
                book_identifier = _query_books(query_str)
            else:
                raise ValueError('Give me a query string or an identifier damnit!')
            self.b_url = book_identifier

            resp = requests.get(book_identifier,headers=rand_headers(AGENTS))
            text = resp.text
            soup = BeautifulSoup(text,'lxml')
            info_main = soup.find('div', class_='BookPage__mainContent')
            info_main_metadat = info_main.find('div', class_='BookPageMetadataSection')
            details = info_main_metadat.find('div', class_='FeaturedDetails')
            self.soup = soup
            self.info_main = info_main
            self.info_main_metadat = info_main_metadat
            self.details = details
            return self
        
        except requests.HTTPError as er:
            print(er)
    
    def get_title(self):
        '''returns title (str) of book.'''
        if not self.info_main:
            return None
        t1 = self.info_main.find('div', class_='BookPageTitleSection__title').find('h1')
        return _check_soup(t1)
    
    def get_id(self):
        '''returns book ID.'''
        return _parse_id(self.b_url)
    
    def get_author_name(self):
        '''returns author name (str) of book'''
        if not self.info_main_metadat:
            return None
        a_n = self.info_main_metadat.find('span', class_='ContributorLink__name')
        return _check_soup(a_n)
    
    def get_author_id(self):
        '''returns author id (str) of book'''
        if not self.info_main_metadat:
            return None
        a_url = self.info_main_metadat.find('a', class_='ContributorLink')
        if a_url:
            a_id = _parse_id(a_url['href'])
            return a_id
        else:
            return None
    
    def get_description(self):
        '''returns description (str) of book'''
        if not self.info_main_metadat:
            return None
        tc = self.info_main_metadat.find('div',class_='TruncatedContent')
        if tc:
            desc = tc.find('span',class_='Formatted')
            if desc:
                description = desc.text.strip()
                if len(description) == 0:
                    description = None
        else:
            description = None
        return description
    
    def get_rating(self):
        '''returns rating (float) of book'''
        if not self.info_main_metadat:
            return None
        b_r = self.info_main_metadat.find('div', class_='RatingStatistics__rating')
        return _check_soup(b_r,'convert to num')

    def get_rating_count(self):
        '''returns ratings count (int) of book'''
        if not self.info_main_metadat:
            return None
        r_c = self.info_main_metadat.find('span', {'data-testid': 'ratingsCount'})
        if r_c:
            rate_count = r_c.text.strip()
        else:
            rate_count = None
            return rate_count
        rate_count = re.sub(r'\,|\sratings|\srating','',rate_count)
        return int(rate_count) if len(rate_count) > 0 else rate_count
    
    def get_ratings_dist(self):
        '''returns discrete ratings distribution (dict) of book'''
        review_stats = self.soup.find('div',class_='RatingsHistogram RatingsHistogram__interactive')
        if not review_stats:
            return None
        rate_dist = {}
        tot_count = 0
        if review_stats:
            for button in review_stats.find_all('div',role='button')[::-1]:
                rating = re.sub(r'\sstars|\sstar','',button['aria-label'])
                count = button.find('div',class_='RatingsHistogram__labelTotal')
                count = re.sub(r'\(.*\)$|,','',count.text.strip())
                count = int(count)
                rate_dist[rating] = count
                
                tot_count += count
            if tot_count == 0:
                return None
            for stars,ct in rate_dist.items():
                rate_dist[stars] = round(ct / tot_count,2)
        return rate_dist

    def get_review_count(self):
        '''returns review count (int) of book'''
        if not self.info_main_metadat:
            return None
        r_c = self.info_main_metadat.find('span', {'data-testid': 'reviewsCount'})
        if r_c:
            rev_count = r_c.text.strip()
        else:
            rev_count = None
            return rev_count
        rev_count = re.sub(r'\,|\sreviews|\sreview','',rev_count)
        return int(rev_count) if len(rev_count) > 0 else rev_count

    def get_top_genres(self):
        '''returns top genres (list) of book'''
        if not self.info_main_metadat:
            return None
        g_l = self.info_main_metadat.find('ul',{'aria-label': 'Top genres for this book'})
        if g_l:
            top_genres = [
                i.find('span', class_ = 'Button__labelItem').text.strip()
                    for i in 
                        g_l.find_all('span', class_ = 'BookPageMetadataSection__genreButton')
            ]
        else:
            top_genres = None
        return top_genres
    
    def get_currently_reading(self):
        '''returns number of users (int) currently reading book'''
        if not self.info_main_metadat:
            return None
        c_r = self.info_main_metadat.find('div', {'data-testid': 'currentlyReadingSignal'})
        if c_r:
            cur_read = c_r.text.strip()
        else:
            cur_read = None
            return cur_read
        cur_read = re.sub(r'people.*$|person.*$','',cur_read)
        return int(cur_read) if len(cur_read) > 0 else cur_read

    def get_want_to_read(self):
        '''returns number of users (int) who want to read the book'''
        if not self.info_main_metadat:
            return None
        w_r = self.info_main_metadat.find('div', {'data-testid': 'toReadSignal'})
        if w_r:
            want_read = w_r.text.strip()
        else:
            want_read = None
            return want_read
        want_read = re.sub(r'people.*$|person.*$','',want_read)
        return int(want_read) if len(want_read) > 0 else want_read

    def get_page_legth(self):
        '''returns page length (int) of book'''
        if not self.details:
            return None
        p_l = self.details.find('p', {'data-testid': 'pagesFormat'})
        if p_l:
            page_length = p_l.text.strip()
            if 'audio cd' in p_l:
                page_length = '0'
        else:
            page_length = None
            return page_length
        page_length = re.sub(r'pages.*$','',page_length)
        return int(page_length) if len(page_length) > 0 else page_length
    
    def get_first_published(self):
        '''returns date (str, form 'DD/MM/YYYY') of when book is first published'''
        if not self.details:
            return None
        f_p = self.details.find('p', {'data-testid': 'publicationInfo'})
        if f_p:
            first_pub = f_p.text.strip().lower()
            first_pub = re.sub(r'^.*published\s','',first_pub)
            first_pub = datetime.strptime(first_pub,'%B %d, %Y').strftime('%m/%d/%Y')
        else:
            first_pub = None
            return first_pub
        return first_pub
    
    def get_similar_books(self):
        '''returns list of similar books, with name/author/url dicts'''
        bklst = self.soup.find('div', class_='BookDiscussions__list')
        if bklst:
            quote_url = bklst.find_all('a',class_='DiscussionCard')[0]['href']
            similar_url = re.sub(r'work/quotes',r'book/similar',quote_url) # the serial id changes from main page to similar page
            similar_books = _get_similar_books(similar_url=similar_url)
        else:
            similar_books = []
        return similar_books
    
    def get_all_data(self)->dict:
        '''returns dict of all scraped data.
        
        returns the following attributes:
        - url: book URL
        - title: book title
        - author: author of book
        - author_url: URL to author's page
        - description: book description
        - rating: book rating (0-5)
        - rating_distribution: distribution of ratings in a dict; 
            - e.g., {'1': 0.02, '2': 0.06, '3': 0.24, '4': 0.42, '5': 0.25}
        - rating_count: number of user ratings given
        - review_count: number of user reviews given
        - top_genres: list of top genres
            - e.g., ['Fiction', 'Historical Fiction', 'Alternate History']
        - currently_reading: number of users currently reading the book
        - want_to_read: number of users wanting to read the book
        - page_length: page length of book
        - first_published: initial book publication date
        - similar_books: list of similar books, with each element being a dict of form {BOOK,URL,AUTHOR}
            - e.g., [{'book': 'The Decline of the West, Vol 2: Perspectives of World History', 
                     'url': 'https://www.goodreads.com/book/show/1659471.The_Decline_of_the_West_Vol_2', 
                     'author': 'Oswald Spengler'},
                     {'book': 'The Enneads', 
                     'url': 'https://www.goodreads.com/book/show/26255.The_Enneads', 
                     'author': 'Plotinus'}]
        '''
        bk_dict = {
            'url': self.b_url,
            'id': self.get_id(),
            'title': self.get_title(),
            'author': self.get_author_name(),
            'author_id': self.get_author_id(),
            'description': self.get_description(),
            'rating': self.get_rating(),
            'rating_distribution': self.get_ratings_dist(),
            'rating_count': self.get_rating_count(),
            'review_count': self.get_review_count(),
            'top_genres': self.get_top_genres(),
            'currently_reading': self.get_currently_reading(),
            'want_to_read': self.get_want_to_read(),
            'page_length': self.get_page_legth(),
            'first_published': self.get_first_published(),
            'similar_books': self.get_similar_books()
        }
        return bk_dict
    
    @staticmethod
    async def multiload_books(books = [], 
                              max_concurrent = 3,
                              write_json=True,
                              json_path='')->list:
        '''loads multiple GoodReads books, returns list of books
        
        :param books: list of book url/identifier strings
        :param max_concurrent: maximum concurrent requests
        :param write_json: if True, write books to json
        :param json_path: if write_json, then specifies file path
        '''
        semaphore = asyncio.Semaphore(max_concurrent)
        async def load1(session, bk1):
            '''loads 1 book'''
            async with semaphore:
                try:
                    book = Alexandria()
                    res = await book.load_book_async(session=session,
                                                        book_identifier=bk1)
                    if res:
                        bkdat = res.get_all_data()
                        return bkdat
                    else:
                        print(f'Failed to load {bk1}')
                        return None
                    
                except asyncio.TimeoutError:
                    print(f'Timeout for {bk1}')
                except Exception as er:
                    print(f'Error for {bk1}: {er}')
                    return None
                
                finally:
                    await asyncio.sleep(0.2)

        connector = aiohttp.TCPConnector(
                                limit=100,
                                limit_per_host=20,
                                ttl_dns_cache=300,
                                use_dns_cache=True
                                )
        
        async with aiohttp.ClientSession(timeout=TIMEOUT,
                                         connector=connector,
                                         headers=rand_headers(AGENTS)) as session:
            tasks = [load1(session,bk) for bk in books]
            bks_res = await asyncio.gather(*tasks,return_exceptions=True)    
        
            successes = [res for res in bks_res if res is not None and not isinstance(res,Exception)]
            fails = len(bks_res) - len(successes)

        bks_dct = {
            'num_success': len(successes),
            'num_fail': fails,
            'results': successes
        }

        if write_json:
            with open(json_path,'w',encoding='utf-8') as jpath:
                json.dump(bks_dct,jpath,indent=4,ensure_ascii=False)

        return successes



