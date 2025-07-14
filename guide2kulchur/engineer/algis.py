import time
import json
import asyncio

import aiohttp
import asyncpg

from guide2kulchur.privateer.insaneasylum import bulk_books_aio, _load_one_book_aio
from guide2kulchur.privateer.alexandria import Alexandria
from guide2kulchur.privateer.recruits import _TIMEOUT

async def test():
    async with asyncpg.create_pool() as pool:
        async with pool.acquire() as con:
            asyncpg.Connection().executemany('',[])