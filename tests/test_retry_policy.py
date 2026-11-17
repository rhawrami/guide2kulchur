import asyncio
from contextlib import redirect_stdout
import io
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, patch

import aiohttp

from guide2kulchur.pipeline.cli import parser
from guide2kulchur.pipeline.http import ResponseError
from guide2kulchur.pipeline.runner import run


class RetryPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def exercise(self, responses):
        class FakeClient:
            instance = None
            def __init__(self, **kwargs):
                FakeClient.instance = self
                self.blocked = None
                self.responses = list(responses)
                self.calls = 0
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            async def fetch(self, *args, **kwargs):
                self.calls += 1
                value = self.responses.pop(0)
                if isinstance(value, BaseException):
                    if isinstance(value, ResponseError) and value.blocked:
                        self.blocked = value
                    raise value
                return value
        with tempfile.TemporaryDirectory() as tmp:
            args = parser().parse_args(['collect', '--kind', 'book', '--ids', '1', '--fetch-only',
                                        '--attempts', '3', '--backoff', '15', '--checkpoint', str(Path(tmp) / 'checkpoint.json')])
            state = {'version': 1, 'kind': 'book', 'similar': False, 'write': False, 'ids': ['1'], 'results': {}}
            with patch('guide2kulchur.pipeline.runner.asyncio.sleep', new_callable=AsyncMock) as sleep, redirect_stdout(io.StringIO()):
                result = await run(None, args, state, client_factory=FakeClient)
            return result, state, FakeClient.instance.calls, [call.args[0] for call in sleep.await_args_list]

    async def test_transient_errors_can_retry_successfully(self):
        for error in (asyncio.TimeoutError(), aiohttp.ClientError('Synthetic connection failure'), ResponseError(503)):
            with self.subTest(error=type(error).__name__):
                result, state, calls, delays = await self.exercise([error, {'rating_count': 1, 'review_count': 0}])
                self.assertEqual((result, calls, delays), (0, 2, [15]))
                self.assertEqual(state['results']['1']['status'], 'success')

    async def test_backoff_is_exponential_and_attempts_are_bounded(self):
        result, state, calls, delays = await self.exercise([ResponseError(500), ResponseError(502), ResponseError(503)])
        self.assertEqual((result, calls, delays), (1, 3, [15, 30]))
        self.assertEqual(state['results']['1']['status'], 'failed')

    async def test_challenges_and_rate_limits_never_retry(self):
        for status in (202, 403, 429):
            with self.subTest(status=status):
                result, state, calls, delays = await self.exercise([ResponseError(status, True)])
                self.assertEqual((result, calls, delays), (2, 1, []))
                self.assertEqual(state['results']['1']['status'], 'blocked')

    async def test_parser_failures_are_not_automatically_retried(self):
        result, state, calls, delays = await self.exercise([ValueError('Synthetic malformed page')])
        self.assertEqual((result, calls, delays), (1, 1, []))
        self.assertEqual(state['results']['1']['status'], 'failed')

    async def test_missing_ids_are_not_retried(self):
        result, state, calls, delays = await self.exercise([ResponseError(404)])
        self.assertEqual((result, calls, delays), (0, 1, []))
        self.assertEqual(state['results']['1']['status'], 'missing')


if __name__ == '__main__':
    unittest.main()
