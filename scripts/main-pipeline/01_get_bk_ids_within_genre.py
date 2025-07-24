import time
import asyncio
import os
import json
from math import log
from typing import (
    Optional, 
    Dict, 
    List,
)

import aiohttp

from guide2kulchur.engineer.plotinus import Plotinus


async def pull_a_genre(semaphore: asyncio.Semaphore,
                       session: aiohttp.ClientSession,
                       what_to_pull: str,
                       genre_name: str,
                       num_attempts: int = 3,
                       see_progress: bool = True) -> Dict[str, Optional[List[str]]]:
    '''return the most read and top shelved books for one genre'''
    async with semaphore:
        neo_p = Plotinus()
        gtype_map = {'most_read': neo_p.get_most_read_this_week,
                     'top_shelved': neo_p.get_top_shelf}
        res = await gtype_map[what_to_pull](session=session,
                                            genre_name=genre_name,
                                            num_attempts=num_attempts,
                                            see_progress=see_progress)
    return (genre_name, res) if res else genre_name
        
        
async def main() -> None:
    '''get the most read books (for this week), and the top-shelved books within each genre'''
    GENRE_URL_PATH = os.path.join('data','genres','genre_URLs.json')
    with open(GENRE_URL_PATH, 'r') as g_url_p:
        genres = json.load(g_url_p)
    genres = genres['results']
    
    CUTOFF = 999    # arbitrary, but need something to filter out small genres
    genres_above_thresh = {i['name'] for i in genres if i['size'] > CUTOFF}    # i don't think there are duplicates, but just in case

    SEM_COUNT = 5
    semaphore = asyncio.Semaphore(SEM_COUNT)
    async with aiohttp.ClientSession() as sesh:
        tasks_most_read = [pull_a_genre(semaphore=semaphore,
                                        session=sesh,
                                        what_to_pull='most_read',
                                        genre_name=g) for g in genres_above_thresh]
        tasks_top_shelved = [pull_a_genre(semaphore=semaphore,
                                          session=sesh,
                                          what_to_pull='top_shelved',
                                          genre_name=g) for g in genres_above_thresh]
        
        BATCH_SIZE = 10
        BATCH_DELAY_SCALAR = 1.2    # somewhat cumbersome towards the end, but it's fine for one-time-run script like this

        in_between_g_types = 60

        for g_type, g_tasks in {'most_read': tasks_most_read,
                                'top_shelved': tasks_top_shelved}.items():
            print(f'\n--------------------------\n| BEGIN :: $${g_type}$$ |\n--------------------------\n')
            
            reqs = 0
            successes = {}
            failures = []

            t_start = time.ctime()
            async for task in asyncio.as_completed(g_tasks):
                reqs += 1   

                if reqs > 0 and reqs % BATCH_SIZE == 0:
                    print('--------------------------- zzz... ---------------------------')
                    time.sleep(BATCH_DELAY_SCALAR ** log(reqs))
                    print('----------------------- ¡guten morgen! -----------------------')

                res = await task
                if isinstance(res, tuple):
                    successes[res[0]] = res[1]
                else:
                    failures.append(res)    # add genre name if failed/no results
                    
                if (PROG_DELIM := 10) and reqs % PROG_DELIM == 0:
                    prog = round(reqs / len(g_tasks), 3)
                    print(f'\n------------------------\n| completed :: <{prog}> |\n------------------------\n')
            t_end = time.ctime()
            
            main_dat = {
                'started_at': t_start,
                'ended_at': t_end,
                'attempts': reqs,
                'sucesses': len(successes),
                'failures_or_empty': len(failures),
                'success_rate': round(len(successes) / reqs, 3),
                'failures_or_empty_items': failures,
                'results': successes
            }
            with open(os.path.join('data', 'genres', f'{g_type}_ids.json'), 'w') as dat2json:
                json.dump(main_dat, dat2json, indent=None)  # reduce file size

            print(f'\n------------------------\n| END :: $${g_type}$$ |\n------------------------\n')
            time.sleep(in_between_g_types)
        

if __name__ == '__main__':
    asyncio.run(main())
    