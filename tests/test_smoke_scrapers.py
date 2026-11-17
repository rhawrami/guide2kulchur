from contextlib import redirect_stdout
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from guide2kulchur.pipeline.cli import parser as collection_parser
from guide2kulchur.pipeline.http import Client, ResponseError
from scripts.maintenance.smoke_scrapers import choose_samples, execute, main, probe


class SmokeTests(unittest.TestCase):
    def test_help_does_not_connect(self):
        with patch('scripts.maintenance.smoke_scrapers.database') as db, redirect_stdout(io.StringIO()), self.assertRaises(SystemExit):
            main(['--help'])
        db.assert_not_called()

    def test_collection_defaults_use_the_conservative_tested_pacing(self):
        args = collection_parser().parse_args(['collect', '--kind', 'book', '--ids', '1'])
        self.assertEqual((args.concurrency, args.request_delay, args.batch_size, args.batch_delay), (1, 5, 10, 15))
        self.assertEqual(Client().delay, 5)

    def test_saved_sample_preview_does_not_fetch(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'samples.json'
            path.write_text(json.dumps({'book': [{'id': '1'}, {'id': '2'}]}))
            with patch('scripts.maintenance.smoke_scrapers.Client') as client, redirect_stdout(io.StringIO()):
                self.assertEqual(main(['--kind', 'book', '--count', '2', '--samples', str(path)]), 0)
            client.assert_not_called()

    def test_diagnostic_id_is_included_in_limit_and_deduplicated(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'samples.json'
            path.write_text(json.dumps({'book': [{'id': '1'}, {'id': '2'}, {'id': '3'}]}))
            args = SimpleNamespace(samples=path, count=2, book_first=['2'])
            self.assertEqual([r['id'] for r in choose_samples(args, ['book'])['book']], ['2', '1'])


class SmokeAsyncTests(unittest.IsolatedAsyncioTestCase):
    async def test_execute_stops_all_kinds_on_challenge(self):
        report = {'stopped_on_access_restriction': True, 'cooldown_seconds': 600,
                  'max_consecutive_pipeline_ok': 0}
        args = SimpleNamespace(count=3)
        samples = {kind: [{'id': '1'}] for kind in ('author', 'user', 'book')}
        with tempfile.TemporaryDirectory() as tmp, redirect_stdout(io.StringIO()):
            with patch('scripts.maintenance.smoke_scrapers.probe', new_callable=AsyncMock, return_value=report) as probe_mock, patch('scripts.maintenance.smoke_scrapers.asyncio.sleep', new_callable=AsyncMock) as sleep:
                code = await execute(args, list(samples), samples, Path(tmp))
            self.assertEqual(code, 1)
            self.assertEqual(probe_mock.await_count, 1)
            self.assertEqual(probe_mock.await_args.args[0], 'author')
            sleep.assert_not_awaited()

    async def test_client_forwards_trace_configs(self):
        trace = MagicMock()
        session = MagicMock()
        session.close = AsyncMock()
        with patch('guide2kulchur.pipeline.http.aiohttp.ClientSession', return_value=session) as create, patch('guide2kulchur.pipeline.http.aiohttp.TCPConnector'):
            async with Client(trace_configs=[trace]):
                self.assertEqual(create.call_args.kwargs['trace_configs'], [trace])
        session.close.assert_awaited_once()

    async def exercise_probe(self, blocked_at=None, filtered_at=None):
        class FakeClient:
            def __init__(self, **kwargs):
                self.trace = kwargs['trace_configs'][0]
                self.headers = {'User-Agent': 'Synthetic test agent'}
                self.cookie_jar = []
                self.blocked = None
                self.count = 0
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            async def fetch(self, kind, identifier):
                self.count += 1
                for fn in self.trace.on_request_start:
                    await fn(self, None, None)
                status = 202 if self.count == blocked_at else 200
                async def text(**kwargs):
                    return '<html><title>Synthetic book</title></html>'
                response = SimpleNamespace(status=status, headers={}, url='https://www.goodreads.com/book/show/' + identifier,
                                           history=[], request_info=SimpleNamespace(headers=self.headers), text=text)
                for fn in self.trace.on_request_end:
                    await fn(self, None, SimpleNamespace(response=response))
                if status == 202:
                    self.blocked = ResponseError(202, True)
                    raise self.blocked
                keys = 'id title author author_id isbn language description image_url rating rating_distribution rating_count review_count top_genres currently_reading want_to_read first_published page_length similar_books_id'.split()
                data = dict.fromkeys(keys)
                data.update(id=identifier, title='Synthetic title', author='Synthetic author',
                            rating_count=0 if self.count == filtered_at else 1, review_count=0)
                return data
        args = SimpleNamespace(request_delay=0, batch_size=2, batch_delay=0, timeout=10)
        with tempfile.TemporaryDirectory() as tmp, redirect_stdout(io.StringIO()):
            return await probe('book', [{'id': str(i)} for i in range(1, 4)], args, Path(tmp), client_factory=FakeClient)

    async def test_probe_counts_consecutive_successes_and_prepares_tuples(self):
        report = await self.exercise_probe()
        self.assertEqual(report['max_consecutive_pipeline_ok'], 3)
        self.assertEqual(report['max_consecutive_http_ok'], 3)
        self.assertEqual(report['max_in_flight'], 1)
        self.assertEqual(report['requests_started'], 3)
        self.assertEqual(report['counts'], {'success': 3})

    async def test_intentionally_filtered_records_are_pipeline_successes(self):
        report = await self.exercise_probe(filtered_at=1)
        self.assertEqual(report['max_consecutive_http_ok'], 3)
        self.assertEqual(report['max_consecutive_pipeline_ok'], 3)
        self.assertEqual(report['max_consecutive_eligible_ok'], 2)
        self.assertEqual(report['counts'], {'filtered': 1, 'success': 2})

    async def test_probe_stops_on_challenge_without_retry(self):
        report = await self.exercise_probe(blocked_at=2)
        self.assertEqual(report['requests_started'], 2)
        self.assertEqual(report['max_consecutive_pipeline_ok'], 1)
        self.assertTrue(report['stopped_on_access_restriction'])
        self.assertEqual(report['counts'], {'success': 1, 'blocked': 1})


if __name__ == '__main__':
    unittest.main()
