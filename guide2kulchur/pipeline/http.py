"""Paced persistent HTTP; challenges stop a run rather than blacklist records."""
import asyncio
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import time
from urllib.parse import urlparse

import aiohttp
from bs4 import BeautifulSoup

from guide2kulchur.engineer.recruits import HouseOfWisdom, Dante, FalseBardiya
from guide2kulchur.engineer.simpullers import _parse_sim_books_page, _parse_sim_authors_page
from guide2kulchur.privateer.recruits import _rand_headers


@dataclass
class ResponseError(Exception):
    status: int
    blocked: bool = False
    retry_after: str | None = None

    def __str__(self):
        return f'HTTP {self.status}' + (' challenge/access restriction/rate limit; stop and review before resuming' if self.blocked else '')

    @property
    def missing(self):
        return self.status in (404, 410) and not self.blocked

    @property
    def cooldown(self):
        try:
            return max(60, float(self.retry_after or 60))
        except ValueError:
            try:
                return max(60, (parsedate_to_datetime(self.retry_after) - datetime.now(timezone.utc)).total_seconds())
            except (TypeError, ValueError, OverflowError):
                return 60


def check_response(status, headers, text, url=None):
    gated = url is not None and urlparse(str(url)).path.rstrip('/') in ('/user/new', '/user/sign_in')
    blocked = (gated or status in (202, 403, 429)
               or headers.get('x-amzn-waf-action', '').lower() == 'challenge'
               or 'AwsWafIntegration' in text or 'token.awswaf.com' in text)
    if status != 200 or blocked:
        raise ResponseError(status, blocked, headers.get('Retry-After'))


class CheckedSession:
    """Per-item proxy: preserve HTTP errors swallowed/wrapped by legacy loaders."""
    def __init__(self, client):
        self.client = client
        self.error = None

    @asynccontextmanager
    async def get(self, url, **kwargs):
        # Legacy loaders randomize every request. The new runner chooses once per
        # session; no newer profile is silently substituted after our mixed trials.
        kwargs.pop('headers', None)
        kwargs.pop('timeout', None)
        try:
            await self.client.pace()
        except ResponseError as exc:
            self.error = exc
            raise
        async with self.client.session.get(url, **kwargs) as response:
            text = await response.text(errors='replace')
            try:
                check_response(response.status, response.headers, text, getattr(response, 'url', None))
            except ResponseError as exc:
                self.error = exc
                if exc.blocked:
                    self.client.blocked = exc
                raise
            yield response


class Client:
    def __init__(self, *, concurrency=1, delay=2, timeout=30, user_agent=None):
        self.concurrency, self.delay, self.timeout = concurrency, delay, timeout
        self.headers = _rand_headers()
        if user_agent:
            self.headers['User-Agent'] = user_agent
        self.blocked = None
        self.lock = asyncio.Lock()
        self.next_request = 0

    async def __aenter__(self):
        self.session = aiohttp.ClientSession(
            headers=self.headers, timeout=aiohttp.ClientTimeout(total=self.timeout),
            connector=aiohttp.TCPConnector(limit=self.concurrency, limit_per_host=self.concurrency),
        )
        return self

    async def __aexit__(self, *exc):
        await self.session.close()

    async def pace(self):
        async with self.lock:
            if self.blocked:
                raise self.blocked
            await asyncio.sleep(max(0, self.next_request - time.monotonic()))
            # A response from another in-flight request may have blocked the run.
            if self.blocked:
                raise self.blocked
            self.next_request = time.monotonic() + self.delay

    async def fetch(self, kind, identifier, *, similar=False):
        proxy = CheckedSession(self)
        if similar:
            url = f'https://www.goodreads.com/{kind}/similar/{identifier}'
            async with proxy.get(url) as response:
                text = await response.text()
            heading = BeautifulSoup(text, 'lxml').find('h1')
            expected = 'readers who enjoyed' if kind == 'book' else 'members who read books by'
            if heading is None or heading.get_text(' ', strip=True).lower() != expected:
                raise ValueError('Unrecognized recommendation page; refusing to store an empty list')
            parser = _parse_sim_books_page if kind == 'book' else _parse_sim_authors_page
            values = parser(text)
            if any(not isinstance(value, str) or not value.isascii() or not value.isdigit() for value in values):
                raise ValueError('Invalid recommendation IDs')
            return values
        scraper = {'book': HouseOfWisdom, 'author': Dante, 'user': FalseBardiya}[kind]()
        try:
            await scraper.load_it_async(session=proxy, item_id=identifier, see_progress=False)
            data = scraper.get_all_data()
        except Exception:
            if proxy.error:
                raise proxy.error from None
            raise
        required = {'book': ('id', 'title', 'author'), 'author': ('author_id', 'author_name'),
                    'user': ('user_id', 'user_name')}[kind]
        if any(not data.get(key) for key in required):
            raise ValueError('Parser returned missing required fields')
        if data[required[0]] != identifier:
            raise ValueError('Parsed ID does not match requested ID')
        return data
