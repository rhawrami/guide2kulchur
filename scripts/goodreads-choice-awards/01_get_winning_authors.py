import os
import asyncio
import json
import time
import re
from typing import (
    Optional, 
    Dict, 
    Any,
)

import aiohttp

from guide2kulchur.privateer.pound import Pound
from guide2kulchur.privateer.recruits import _TIMEOUT

# NB: THIS SCRIPT SHOULD BE RAN AFTER 'get_awards_data.py'
# Now that we have the award winning books, we can now pull
# data on the winning authors. For each year of data, we'll 
# get a set of the winning author IDs, and pull data on them.

async def _pull_one_author(session: aiohttp.ClientSession,
                           semaphore: asyncio.Semaphore,
                           author_id: str,
                           awarded_book_id: str,
                           awarded_book: str,
                           award_year: int,
                           award_category: str,
                           award_num_votes: int) -> Optional[Dict[str,Any]]:
    '''Pull one PUBLICLY AVAILABLE Goodreads award winning author'''
    async with semaphore:
        try:
            pnd = Pound()
            await pnd.load_author_async(session=session,
                                        author_identifier=author_id,
                                        see_progress=False)
        except (asyncio.TimeoutError, aiohttp.ClientError): # retry once
            try:
                print(f'RETRYING: {author_id} :: {award_category} ---- {time.ctime()}')
                SLEEP_FOR = 5
                await asyncio.sleep(SLEEP_FOR)
                pnd = Pound()
                await pnd.load_author_async(session=session,
                                            author_identifier=author_id,
                                            see_progress=False)
            except Exception:
                print(f'FAILED: {author_id} :: {award_category} ---- {time.ctime()}')
                return None
        except Exception:
            print(f'FAILED: {author_id} :: {award_category} ---- {time.ctime()}')
            return None
        
        pnd_dat = pnd.get_all_data(to_dict=True)
        
        pnd_dat['awarded_book_id'] = awarded_book_id
        pnd_dat['awarded_book'] = awarded_book
        pnd_dat['award_year'] = award_year
        pnd_dat['award_category'] = award_category
        pnd_dat['award_num_votes'] = award_num_votes

        print(f'SUCCESS: {author_id} :: {pnd_dat['name']} :: {award_category} ---- {time.ctime()}')
        return pnd_dat
        

async def main() -> None:
    '''Collect multiple years of PUBLICLY AVAILABLE Goodreads Annual Choice Awards winning authors'''
    MAIN_DIR = 'data'
    AWARD_DIR = 'goodreads-choice-awards'
    AWARD_SUB_DIR = 'authors'
    os.makedirs(os.path.join(MAIN_DIR,AWARD_DIR,AWARD_SUB_DIR), exist_ok=True)

    MIN,MAX = 2011,2024
    prompt = f'''
Hello there. This script allows you to scrape Goodreads Annual Choice Awards winning authors data for a collection
of years. The data collected will be placed in the 'data/goodreads-choice-awards/authors/' directory. For a preview of
what the awards look like, please use this link: https://www.goodreads.com/choiceawards/best-books-2024

To begin, please enter the collection of award years you'd like to scrape. NB:

- Award data is available from 2011 to 2024, as of {time.ctime()}.
- Separate the years by a space; press Enter when you are done.

NB NB:

- YOU SHOULD HAVE ALREADY COLLECTED THE AWARDS DATA AND PLACED IT IN 'data/goodreads-choice-awards/books/'

Enter years here: '''
    user_resp = input(prompt)
    sep_resp = user_resp.split(' ')
    years_to_pull = []
    for yr_resp in sep_resp:
        try:
            int_yr = int(yr_resp)
            if not MIN <= int_yr <= MAX:
                continue
        except ValueError:
            continue
        years_to_pull.append(int_yr)

    if not len(years_to_pull):
        print('Input Error: no years recognized from input. Try again.')
        return None
    else:
        confirmation = input(f'You have requested the following years:\n{years_to_pull}\nIs this correct? Please enter "YES"/"yes"/"Y"/"y" to confirm: ')
        if not confirmation.lower() in ['yes','y']:
            print('Please restart script and reinput correct years.\n')
            return None

    winner_books_dir = os.listdir(os.path.join(MAIN_DIR,AWARD_DIR,'books'))
    file_match_pattern = r'|'.join([str(yr) for yr in years_to_pull])
    files_iter = [file for file in winner_books_dir if re.search(file_match_pattern,file)]
    files_iter = sorted(files_iter)
    
    for f_p in files_iter:
        with open(os.path.join(MAIN_DIR,AWARD_DIR,'books',f_p),'r') as file:
            data = json.load(file)
        winning_books = data['results']
        year = int(data['year'])

        SEM = 3
        semaphore = asyncio.Semaphore(SEM)
        connector = aiohttp.TCPConnector(
                            limit=20,
                            limit_per_host=5,
                            ttl_dns_cache=300,
                            use_dns_cache=True
                            )
        
        async with aiohttp.ClientSession(timeout=_TIMEOUT,
                                            connector=connector) as sesh:
            print(f'\n------------ATTEMPTING {year} WINNING AUTHORS------------')
            tasks = []
            for bk in winning_books:
                author_id = bk['author_id']
                awarded_book_id = bk['id']
                awarded_book = bk['title']
                award_category = bk['award_category']
                award_num_votes = bk['award_num_votes']
                
                fn = _pull_one_author(session=sesh,
                                      semaphore=semaphore,
                                      author_id=author_id,
                                      awarded_book=awarded_book,
                                      awarded_book_id=awarded_book_id,
                                      award_year=year,
                                      award_category=award_category,
                                      award_num_votes=award_num_votes)
                tasks.append(fn)
            
            ctr = 0
            bulk_dat = []
            SLEEPER_UNIT = 1
            BATCH_DELIM = 5
            async for poundian in asyncio.as_completed(tasks):
                if ctr > 0 and ctr % BATCH_DELIM == 0:
                    print('------ sleep zzz... ------')
                    time.sleep(SLEEPER_UNIT)
                    print('------ waking up... ------')
                result = await poundian
                if not result:
                    continue
                else:
                    bulk_dat.append(result)
                ctr += 1
                
                if ctr == len(tasks) or ctr % 10 == 0:
                    success_rate = round(len(bulk_dat) / ctr, 2)
                    PROGRESS_STATEMENT = f'''
 <--------------------------------------------------->
<---- COMPLETED {ctr} / {len(tasks)} ITEMS (SUCC. RATE: {success_rate}) ----->
 <--------------------------------------------------->
'''
                    print(PROGRESS_STATEMENT)
    
        categories_dup = [dat['award_category'] for dat in bulk_dat]
        categories = list(set(categories_dup))
        dat = {
                'year': year,
                'successes': len(bulk_dat),
                'categories': categories,
                'results': bulk_dat
            }
        f_name = os.path.join(MAIN_DIR,AWARD_DIR,AWARD_SUB_DIR,f'winning_authors_{year}.json')
        with open(f_name,'w') as yr_file:
            json.dump(dat,yr_file,indent=4)
        print(f'------------PULLED {year} WINNING AUTHORS------------')
        print(f'------------WRITTEN {year} WINNING AUTHORS TO {f_name}------------\n')
        WAIT_TIME = 30 
        time.sleep(WAIT_TIME)
    
    closing = '''
-----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----
Collection completed; as a final message, I leave you with an excerpt from Titus Burckhardt's Mirror of the Intellect:

"Every creature possesses perfections and imperfections, beauties and uglinesses; whoever disregards their uglinesses 
and mentions only their beauties, truly participates in the Divine Forgiveness. It is related that Jesus— on whom be peace— 
was travelling with his disciples, and that they passed by a dead dog, the evil smell from which was unbearable. 'How foul-smelling 
is this carcass!' exclaimed the disciples. But Jesus replied: 'How beautiful is the whiteness of its teeth!'"
-----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----    -----   -----   -----   -----
'''
    print(closing)
            

if __name__ == '__main__':
    asyncio.run(main())
    