import os
import asyncio

from guide2kulchur.engineer.plato import Plato


async def main() -> None:
    '''get Goodreads Genre names, URLs and book counts.'''
    apology = Plato()
    await apology.get_genre_urls(semaphore_count=3,
                                 num_attempts=4,
                                 batch_delay=1,
                                 batch_size=5,
                                 see_progress=True,
                                 write_json=os.path.join('data','genres','genre_URLs.json'))


if __name__ == '__main__':
    asyncio.run(main())