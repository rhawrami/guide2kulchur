import json
import time
import asyncio
from typing import List, Optional, Dict, Union
from types import SimpleNamespace

import aiohttp

from guide2kulchur.privateer.alexandria import Alexandria
from guide2kulchur.privateer.falsedmitry import FalseDmitry
from guide2kulchur.privateer.pound import Pound
from guide2kulchur.privateer.recruits import _rand_headers, _TIMEOUT, _AGENTS


async def _load_one_book_aio(session: aiohttp.ClientSession,
                             identifer: str,
                             exclude_attrs: Optional[List[str]] = None,
                             see_progress: bool = True,
                             to_dict: bool = False) -> Optional[Union[SimpleNamespace,Dict]]:
            '''load one Goodreads book ASYNC
            
            :session: an aiohttp.ClientSession
            :identifer: a book ID or URL
            :exclude_attrs: book attributes to exclude
            :see_progress: view progress for each book pull
            :to_dict: convert book data to dict; otherwise, stays SimpleNamespace
            '''
            try:
                alx = Alexandria()
                await alx.load_book_async(session=session,
                                          book_identifier=identifer,
                                          see_progress=see_progress)
                if exclude_attrs:
                     if 'similar_books' in exclude_attrs:
                          bk_dat = alx.get_all_data(exclude_attrs=exclude_attrs,
                                                    to_dict=to_dict)
                else:
                    bk_dat = await alx.get_all_data_async(session=session,
                                                          exclude_attrs=exclude_attrs,
                                                          to_dict=to_dict)
                return bk_dat
            
            except Exception:
                return None


async def _load_one_user_aio(session: aiohttp.ClientSession,
                             identifer: str,
                             exclude_attrs: Optional[List[str]] = None,
                             see_progress: bool = True,
                             to_dict: bool = False) -> Optional[Union[SimpleNamespace,Dict]]:
            '''load one Goodreads user ASYNC
            
            :session: an aiohttp.ClientSession
            :identifer: a user ID or URL
            :exclude_attrs: user attributes to exclude
            :see_progress: view progress for each user pull
            :to_dict: convert user data to dict; otherwise, stays SimpleNamespace
            '''
            try:
                dmitry = FalseDmitry()
                await dmitry.load_user_async(session=session,
                                             user_identifier=identifer,
                                             see_progress=see_progress)
                usr_dat = dmitry.get_all_data(exclude_attrs=exclude_attrs,
                                              to_dict=to_dict)
                return usr_dat
                
            except Exception:
                return None


async def _load_one_author_aio(session: aiohttp.ClientSession,
                               identifer: str,
                               exclude_attrs: Optional[List[str]] = None,
                               see_progress: bool = True,
                               to_dict: bool = False) -> Optional[Union[SimpleNamespace,Dict]]:
            '''load one Goodreads author ASYNC
            
            :session: an aiohttp.ClientSession
            :identifer: a author ID or URL
            :exclude_attrs: author attributes to exclude
            :see_progress: view progress for each author pull
            :to_dict: convert author data to dict; otherwise, stays SimpleNamespace
            '''
            try:
                pnd = Pound()
                await pnd.load_author_async(session=session,
                                            author_identifier=identifer,
                                            see_progress=see_progress)
                authr_dat = pnd.get_all_data(exclude_attrs=exclude_attrs,
                                             to_dict=to_dict)
                return authr_dat
                
            except Exception:
                return None


async def bulk_load_aio(category: str,
                        identifiers: List[str],
                        exclude_attrs: Optional[List[str]] = None,
                        semaphore: int = 3,
                        to_dict: bool = False,
                        see_progress: bool = True,
                        write_json: Optional[str] = None):
    '''Collect multiple PUBLICLY AVAILABLE Goodreads units asynchronously.
    
    :param category: category to pull from; options include ['book', 'user', 'author']
    :param identifiers: unique item identifiers, or unique URLs
    :param exclude_attrs: item attributes to exclude
    :param semaphore: semaphore control; defaults to three requests
    :param to_dict: converts data to dict type; otherwise, stays SimpleNamespace
    :param see_progress: view per-unit progress
    :param write_json: file_name to write data to json
    '''
    cat = category.lower()
    if cat not in ['book', 'user', 'author']:
         raise ValueError('category must be one of the three: ["book", "user", "author"]')
    
    cat_fn_map = {
        'book': _load_one_book_aio,
        'user': _load_one_user_aio,
        'author': _load_one_author_aio
    }
    cat_fn = cat_fn_map[category]
    bulk_data = []
    async with asyncio.Semaphore(semaphore):
        async with aiohttp.ClientSession(headers=_rand_headers(_AGENTS),
                                         timeout=_TIMEOUT) as sesh:
            tasks = [cat_fn(session=sesh,
                            identifer=id_,
                            exclude_attrs=exclude_attrs,
                            see_progress=see_progress,
                            to_dict=to_dict) for id_ in identifiers]
            
            time_start = time.ctime()
            async for item in asyncio.as_completed(tasks):
                result = await item
                if not result: 
                    if see_progress:
                            print(f'Error for this')
                    continue
                else:
                    bulk_data.append(result)
            time_end = time.ctime()
    
    attempted = len(identifiers)
    successes = len(bulk_data)
    failures = len(identifiers) - successes
    success_rate = successes / attempted
    if write_json:
        data_to_write = bulk_data if to_dict else [bk.__dict__ for bk in bulk_data]
        json_dat = {
            'category': cat,
            'query_start': time_start,
            'query_end': time_end,
            'attempted': attempted,
            'successes': successes,
            'failures': failures,
            'success_rate': success_rate,
            'results': data_to_write
        }
        with open(write_json,'w') as json_file:
            json.dump(json_dat,json_file,indent=4)

    metadat = f'''
------------------------------------
category: {cat}
started at: {time_start}
ended at: {time_end}
attempted: {attempted}
successes: {successes}
failures: {failures}
success rate: {success_rate}
------------------------------------
'''
    print(metadat)

    return bulk_data
                        

async def bulk_books_aio(book_ids: List[str],
                         exclude_attrs: Optional[List[str]] = None,
                         semaphore: int = 3,
                         to_dict: bool = False,
                         see_progress: bool = True,
                         write_json: Optional[str] = None):
    '''Collect data on multiple PUBLICLY AVAILABLE Goodreads books asynchronously.
    
    :param book_ids: unique book identifiers, or book URLs
    :param exclude_attrs: book attributes to exclude; see below for options
    :param semaphore: semaphore control; defaults to three requests
    :param to_dict: converts data to dict type; otherwise, stays SimpleNamespace
    :param see_progress: view per-book progress
    :param write_json: file_name to write data to json

    ------------------------------------------------------------------------------------------
    Data returned will by default include the following attributes:
    - **url** (str): URL to Goodreads book page
    - **id** (str): unique Goodreads book ID
    - **title** (str): book title
    - **author** (str): name of author of book
    - **author_id** (str): unique Goodreads author ID
    - **image_url** (str): URL to book's cover image
    - **description** (str): book description
    - **rating**: book's average rating (1-5)
    - **rating_distribution** (Dict[str,float]): book's rating's distribution; 
        - e.g., {'1': 0.02, '2': 0.06, '3': 0.24, '4': 0.42, '5': 0.25}
    - **rating_count** (int): number of user ratings given
    - **review_count** (int): number of user reviews given
    - **top_genres** (List[str]): list of top genres
        - e.g., ['Fiction', 'Historical Fiction', 'Alternate History']
    - **currently_reading** (int): number of Goodreads users currently reading the book
    - **want_to_read** (int): number of Goodreads users wanting to read the book
    - **page_length** (int): page length of book
    - **first_published** (str): book's initial publication date (in "MM/DD/YYYY" format)
    - **similar_books** (List[Dict]): list of similar books, with each element being a Dict of title/id/author_name

    To override and exclude any of the attributes, include the attribute name in the 'exclude_attrs' param.
    - e.g., to exclude top_genres and author_id, set exclude_attrs = ['top_genres', 'author_id']
    '''
    await bulk_load_aio(category='book',
                        identifiers=book_ids,
                        exclude_attrs=exclude_attrs,
                        semaphore=semaphore,
                        to_dict=to_dict,
                        see_progress=see_progress,
                        write_json=write_json)


async def bulk_users_aio(user_ids: List[str],
                         exclude_attrs: Optional[List[str]] = None,
                         semaphore: int = 3,
                         to_dict: bool = False,
                         see_progress: bool = True,
                         write_json: Optional[str] = None):
    '''Collect data on multiple PUBLICLY AVAILABLE Goodreads users asynchronously.
    
    :param book_ids: unique user identifiers, or user URLs
    :param exclude_attrs: user attributes to exclude; see below for options
    :param semaphore: semaphore control; defaults to three requests
    :param to_dict: converts data to dict type; otherwise, stays SimpleNamespace
    :param see_progress: view per-user progress
    :param write_json: file_name to write data to json

    ------------------------------------------------------------------------------------------
    Data returned will by default include the following attributes:
    - **url** (str): URL to Goodreads user page
    - **id** (str): unique Goodreads user ID
    - **name** (str): user's name
    - **image_url** (str): URL to user's profile picture
    - **rating** (float): average of book ratings given by user (1-5)
    - **rating_count** (int): number of user ratings given
    - **review_count** (int): number of user reviews given
    - **favorite_genres** (List[str]): list of user's favorite genres
        - e.g., ['Fiction', 'Historical Fiction', 'Alternate History']
    - **currently_reading_sample** (List[Dict]): sample list of books that user is currently reading
    - **quotes_sample** (List[Dict]): sample list of quotes selected by user (note that this is dynamic)
    - **follower_count** (int): number of users that are following the loaded user
    - **friend_count** (int): number of users that user is friends with
    - **friends_sample** (List[Dict]): sample list of user's friends
    - **followings_sample** (List[Dict]): sample list of user's followings

    To override and exclude any of the attributes, include the attribute name in the 'exclude_attrs' param.
    - e.g., to exclude favorite_genres and friend_count, set exclude_attrs = ['favorite_genres', 'friend_count']
    '''
    await bulk_load_aio(category='user',
                        identifiers=user_ids,
                        exclude_attrs=exclude_attrs,
                        semaphore=semaphore,
                        to_dict=to_dict,
                        see_progress=see_progress,
                        write_json=write_json)


async def bulk_authors_aio(author_ids: List[str],
                           exclude_attrs: Optional[List[str]] = None,
                           semaphore: int = 3,
                           to_dict: bool = False,
                           see_progress: bool = True,
                           write_json: Optional[str] = None):
    '''Collect data on multiple PUBLICLY AVAILABLE Goodreads authors asynchronously.
    
    :param author_ids: unique author identifiers, or author URLs
    :param exclude_attrs: author attributes to exclude; see below for options
    :param semaphore: semaphore control; defaults to three requests
    :param to_dict: converts data to dict type; otherwise, stays SimpleNamespace
    :param see_progress: view per-author progress
    :param write_json: file_name to write data to json

    ------------------------------------------------------------------------------------------
    Data returned will by default include the following attributes:
    - **url** (str): URL to Goodreads author page
    - **id** (str): unique Goodreads author ID
    - **name** (str): author's name
    - **description** (str): description of author
    - **image_url** (str): URL to author's cover picture
    - **birth_place** (str): author's place of birth
    - **birth** (str): author's birth date (in "MM/DD/YYYY" format)
    - **death** (str): author's death date (in "MM/DD/YYYY" format)
    - **top_genres** (List[str]): list of author's favorite genres
        - e.g., ['Fiction', 'Historical Fiction', 'Alternate History']
    - **rating** (str): average of user ratings given to author's works (1-5)
    - **rating_count** (int): number of user ratings given to author's works
    - **review_count** (int): number of user reviews given to author's works
    - **follower_count** (int): number of users are following the author
    - **influences** (List[Dict]): list of other authors that current author is influenced by 
    - **sample_books** (List[Dict]): sample (max n = 10) of loaded Goodreads author's most popular books

    To override and exclude any of the attributes, include the attribute name in the 'exclude_attrs' param.
    - e.g., to exclude birth_place and influences, set exclude_attrs = ['birth_place', 'influences']
    '''
    await bulk_load_aio(category='author',
                        identifiers=author_ids,
                        exclude_attrs=exclude_attrs,
                        semaphore=semaphore,
                        to_dict=to_dict,
                        see_progress=see_progress,
                        write_json=write_json)
    

if __name__ == '__main__':
    items = ['29963', '193536', '5725109']
    
    async def main():
         await bulk_authors_aio(author_ids=items,
                                semaphore=2,
                                to_dict=False,
                                see_progress=False,
                                write_json='testfile.json')
    asyncio.run(main())