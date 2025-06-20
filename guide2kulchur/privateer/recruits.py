import requests
from bs4 import BeautifulSoup
import random
import re
import aiohttp

'''
recruits.py will be where we put
our helper functions, hence the name :)
'''

AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15'
]

def rand_headers(agents=AGENTS):
    header = {
        'User-Agent': random.choice(AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': random.choice(['en-US,en;q=0.9', 'en-US,en;q=0.8', 'en-GB,en;q=0.9']),
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1'
    }
    if random.choice([True,False]):
        header['DNT'] = '1'
    return header

TIMEOUT = aiohttp.ClientTimeout(total=30,
                                connect=15,
                                sock_read=30)

def _check_soup(sp,other_opr=None):
    '''checks if soup is empty; if not, returns text'''
    if sp:
        s = sp.text.strip()
        if other_opr == 'convert to num':
            s = float(s)
    else:
        s = None
    return s

def _parse_id(url=''):
    '''parses Goodreads book or author url for unique ID,returns ID string'''
    id_ = re.findall(r'\d+',url)
    if len(id_) > 0:
        return id_[0]
    else:
        return None

def _query_books(search_str=''):
    '''returns the url to to top resulted book page from a query'''
    try:
        r = requests.get('https://www.goodreads.com/search',
                         headers=rand_headers(),
                         params={'q': search_str})
        soup = BeautifulSoup(r.text,'lxml')
        tbl = soup.find('table',class_ = 'tableList')
        if tbl:
            res = tbl.find('tr').find('a')['href']
            res = re.sub(r'\?.*','',res)
            top_result = 'https://www.goodreads.com' + res
            return top_result
        else:
            raise LookupError(f'No results shown for query: "{search_str}"')
    except requests.HTTPError as er:
        raise er

async def _query_books_async(session=aiohttp.ClientSession,search_str=''):
    '''returns the url to to top resulted book page from a query'''
    try:
        async with session.get('https://www.goodreads.com/search',
                               timeout=TIMEOUT,
                               headers=rand_headers(),
                               params={'q': search_str}) as resp:
            text = await resp.text()
            soup = BeautifulSoup(text,'lxml')
            tbl = soup.find('table',class_ = 'tableList')
            if tbl:
                res = tbl.find('tr').find('a')['href']
                res = re.sub(r'\?.*','',res)
                top_result = 'https://www.goodreads.com' + res
                return top_result
            else:
                raise LookupError(f'No results shown for query: "{search_str}"')
    except aiohttp.ClientError as er:
        raise er
        
def _get_similar_books(similar_url='')->list:
    '''returns similar book data

    :param similar_url: original GoodReads book url 

    Returns list of dictionaries of similar books, of the form:\n
    [{'book': BOOK_TITLE, 'url': book_identifier, 'author': BOOK_AUTHOR},...]
    '''
    try:
        r = requests.get(similar_url,headers=rand_headers())
        soup = BeautifulSoup(r.text,'lxml')
        dat = []
        bklist = soup.find_all('div',class_='responsiveBook')

        if not bklist:
            return None
        
        for idx,book in enumerate(bklist):
            if idx == 0:
                continue # this is the original book
            else:
                b_url = 'https://www.goodreads.com' + book.find('a',itemprop='url')['href']
                b_id = _parse_id(b_url)
                b_title = book.find_all('span',itemprop='name')[0].text.strip()
                b_author = book.find_all('span',itemprop='name')[1].text.strip()
                dat.append({
                    'id': b_id,
                    'title': b_title,
                    'author': b_author
                })
        return dat

    except requests.HTTPError as er:
        print(er)
        return None

async def _get_similar_books_async(session=aiohttp.ClientSession,similar_url='')->list:
    '''returns similar book data

    :param similar_url: original GoodReads book url 

    Returns list of dictionaries of similar books, of the form:\n
    [{'book': BOOK_TITLE, 'url': book_identifier, 'author': BOOK_AUTHOR},...]
    '''
    try:
        async with session.get(similar_url,headers=rand_headers()) as resp:
            text = await resp.text()
            soup = BeautifulSoup(text,'lxml')
            dat = []
            for idx,book in enumerate(soup.find_all('div',class_='responsiveBook')):
                if idx == 0:
                    continue # this is the original book
                else:
                    b_url = 'https://www.goodreads.com' + book.find('a',itemprop='url')['href']
                    b_title = book.find_all('span',itemprop='name')[0].text.strip()
                    b_author = book.find_all('span',itemprop='name')[1].text.strip()
                    dat.append({
                        'book': b_title,
                        'url': b_url,
                        'author': b_author
                    })
            return dat
    except aiohttp.ClientError as er:
        print(er)
        return None
    
    
    

            

    