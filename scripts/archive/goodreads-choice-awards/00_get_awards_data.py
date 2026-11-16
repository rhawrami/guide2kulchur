import os
import asyncio
import json
import time

import aiohttp

from guide2kulchur.privateer.herodotus import Herodotus
from guide2kulchur.privateer.recruits import _TIMEOUT

# In this script, we'll be pulling multiple years of 
# Goodreads Annual Choice Awards book data
# We'll be scraping years sequentially, but we'll be pulling
# data within each year asynchronously

async def main() -> None:
    '''collect multiple years of PUBLICLY AVAILABLE Goodreads book award data, write annual data to json'''
    MAIN_DIR = 'data'
    AWARD_DIR = 'goodreads-choice-awards'
    AWARD_SUB_DIR = 'books'
    os.makedirs(os.path.join(MAIN_DIR,AWARD_DIR,AWARD_SUB_DIR), exist_ok=True)

    MIN,MAX = 2011,2024
    prompt = f'''
Hello there. This script allows you to scrape Goodreads Annual Choice Awards book data for a collection
of years. The data collected will be placed in the 'data/goodreads-choice-awards/books/' directory. For a preview of
what the awards look like, please use this link: https://www.goodreads.com/choiceawards/best-books-2024

To begin, please enter the collection of award years you'd like to scrape. NB:

- Award data is available from 2011 to 2024, as of {time.ctime()}.
- Separate the years by a space; press Enter when you are done.

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

    SEMAPHORE_C = 4
    for yr in years_to_pull:
        connector = aiohttp.TCPConnector(
                            limit=20,
                            limit_per_host=5,
                            ttl_dns_cache=300,
                            use_dns_cache=True
                            )
        async with aiohttp.ClientSession(timeout=_TIMEOUT,
                                         connector=connector) as session:
            print(f'\n------------ATTEMPTING {yr} AWARDS------------')
            hero = Herodotus(semaphore=SEMAPHORE_C)
            yr_res = await hero.pull_one_year(session=session,
                                        year=yr)
            successes = [res for res in yr_res 
                            if res is not None 
                            and not isinstance(res,Exception)]
            categories_dup = [suc['award_category'] for suc in successes]
            categories = list(set(categories_dup))
            dat = {
                'year': yr,
                'successes': len(successes),
                'categories': categories,
                'results': successes
            }
            
            f_name = os.path.join(MAIN_DIR,AWARD_DIR,AWARD_SUB_DIR,f'winners_{yr}.json')
        with open(f_name,'w') as yr_file:
            json.dump(dat,yr_file,indent=4)
        print(f'------------PULLED {yr} AWARDS------------')
        print(f'------------WRITTEN {yr} AWARDS TO {f_name}------------\n')
        WAIT_TIME = 30 # long, but I'm trying to be nice :)
        time.sleep(WAIT_TIME)
    
    closing = '''
-----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   ----- 
Scraping process completed; as a final message, I leave you with an excerpt from Jünger's Storm of Steel:

"That was the final winnings in a game on which so often all had been staked: the nation was no longer for me 
an empty thought veiled in symbols; and how could it have been otherwise when I had seen so many die for its sake, 
and been schooled myself to stake my life for its credit every minute, day and night, without a thought? And so, 
strange as it may sound, I learned from this very four years' schooling in force and in all the fantastic extravagance 
of material warfare that life had no depth of meaning except when it is pledged for an ideal, and that there are ideals in 
comparison with which the life of an individual and even of a people has no weight. And though the aim for which I fought as an 
individual, as an atom in the whole body of the army, was not to be achieved, though material force cast us, apparently, to the 
earth, yet we learned once and for all to stand for a cause and if necessary to fall as befitted men."
-----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   -----   ----- 
'''
    print(closing)


if __name__=='__main__':
    asyncio.run(main())