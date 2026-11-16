import asyncio
from contextlib import asynccontextmanager
import gzip
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from guide2kulchur.pipeline.cli import parser, main
from guide2kulchur.pipeline.common import atomic_json, database
from guide2kulchur.pipeline.http import Client, ResponseError, check_response
from guide2kulchur.pipeline.runner import load_checkpoint, pending, persist, run
from guide2kulchur.pipeline.sources import discover, file_ids, missing_ids, normalize_id

FIXTURES = Path(__file__).parent / 'fixtures'


class SourcesTests(unittest.TestCase):
    def test_ids_and_urls(self):
        self.assertEqual(normalize_id('00123', 'book'), '123')
        self.assertEqual(normalize_id('https://www.goodreads.com/book/show/123-example', 'book'), '123')
        for value in ('x123', 'https://evil.invalid/book/show/123', 'https://www.goodreads.com/user/show/123', '-1'):
            with self.subTest(value=value), self.assertRaises(ValueError):
                normalize_id(value, 'book')

    def test_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'ids.txt.gz'
            with gzip.open(path, 'wt') as stream:
                stream.write('# seeds\n123\n456\n')
            self.assertEqual(list(file_ids(path)), ['123', '456'])
            path = Path(tmp) / 'genres.json'
            path.write_text(json.dumps({'results': {'fiction': ['123', '456']}}))
            self.assertEqual(list(file_ids(path)), ['123', '456'])
            path.write_text(json.dumps({'results': [{'id': '123'}]}))
            self.assertEqual(list(file_ids(path)), ['123'])

    def test_bounded_filtering_and_deduplication(self):
        conn = MagicMock()
        conn.execute.return_value.fetchall.side_effect = [[('123',)], [('456',)]]
        self.assertEqual(missing_ids(conn, 'book', ['123', '123', '999', '456'], 2), ['123', '456'])
        self.assertEqual(conn.execute.call_count, 2)
        self.assertNotIn('TRUNCATE', conn.execute.call_args.args[0].as_string())

    def test_discovery_allowlist_and_sql_parameters(self):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [('123',)]
        self.assertEqual(discover(conn, 'book', 'user-reading-books', 10, '2025-11-01'), ['123'])
        query, params = conn.execute.call_args.args
        self.assertIn('unnest("currently_reading_sample_books")', query.as_string())
        self.assertEqual(params, ['2025-11-01', 'book', 10])
        with self.assertRaises(ValueError):
            discover(conn, 'user', 'book-authors', 10)

    def test_invalid_cli_never_connects(self):
        with patch('guide2kulchur.pipeline.cli.database') as db:
            with self.assertRaises(SystemExit):
                main(['collect', '--kind', 'book', '--ids', '123', '--batch-size', '0'])
            db.assert_not_called()

    def test_preview_has_no_network_or_checkpoint(self):
        with patch('guide2kulchur.pipeline.cli.database') as db, patch('guide2kulchur.pipeline.cli.missing_ids', return_value=['123']), patch('guide2kulchur.pipeline.cli.run') as runner:
            self.assertEqual(main(['collect', '--kind', 'book', '--ids', '123']), 0)
            self.assertFalse(db.call_args.kwargs['write'])
            runner.assert_not_called()

    def test_atomic_checkpoint_and_mode_validation(self):
        state = {'version': 1, 'kind': 'book', 'similar': False, 'write': False,
                 'ids': ['123', '456'], 'results': {'123': {'status': 'success'}}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'checkpoint.json'
            atomic_json(path, state)
            self.assertEqual(pending(load_checkpoint(path, 'book', False, False)), ['456'])
            with self.assertRaises(ValueError):
                load_checkpoint(path, 'book', False, True)

    def test_readonly_connection_enforces_autocommit_safety(self):
        with patch('guide2kulchur.pipeline.common.load_dotenv'), patch.dict('os.environ', {'PG_STRING': 'dbname=test'}), patch('guide2kulchur.pipeline.common.psycopg.connect') as connect:
            conn = database()
            self.assertTrue(conn.read_only)
            self.assertEqual(conn.execute.call_args_list[0].args[0], 'SET default_transaction_read_only = on')

    def test_only_missing_ids_are_blacklisted(self):
        conn = MagicMock()
        records = [{'id': '123', 'status': 'blocked'}, {'id': '456', 'status': 'failed'},
                   {'id': '789', 'status': 'missing'}]
        persist(conn, 'book', records)
        conn.execute.assert_called_once()
        self.assertEqual(conn.execute.call_args.args[1], ('789', 'book'))

    def test_recommendation_404_does_not_blacklist_entity(self):
        conn = MagicMock()
        records = [{'id': '123', 'status': 'missing'}]
        persist(conn, 'book', records, similar=True)
        conn.execute.assert_not_called()
        self.assertEqual(records[0]['status'], 'failed')


class HttpTests(unittest.IsolatedAsyncioTestCase):
    def test_challenge_and_status_classification(self):
        for status, headers, text in [(202, {}, ''), (429, {}, ''), (403, {}, ''),
                                      (200, {'x-amzn-waf-action': 'challenge'}, ''),
                                      (200, {}, (FIXTURES / 'challenge.html').read_text())]:
            with self.subTest(status=status), self.assertRaises(ResponseError) as caught:
                check_response(status, headers, text)
            self.assertTrue(caught.exception.blocked)
            self.assertFalse(caught.exception.missing)
        with self.assertRaises(ResponseError) as caught:
            check_response(404, {}, 'Not found')
        self.assertTrue(caught.exception.missing)
        check_response(200, {}, '<html>ok</html>')
        self.assertEqual(ResponseError(429, True, '120').cooldown, 120)

    async def test_actual_loaders_with_fixtures_and_stable_headers(self):
        for kind, identifier in [('book', '123'), ('author', '42'), ('user', '7')]:
            response = SimpleNamespace(status=200, headers={})
            async def text(**kwargs):
                return (FIXTURES / f'{kind}.html').read_text()
            response.text = text
            @asynccontextmanager
            async def get(url, **kwargs):
                self.assertNotIn('headers', kwargs)
                yield response
            client = Client(delay=0)
            client.session = SimpleNamespace(get=get)
            data = await client.fetch(kind, identifier)
            self.assertEqual(data['rating_count'], 123)
            conn = MagicMock()
            persist(conn, kind, [{'id': identifier, 'status': 'success', 'data': data}])
            insert = conn.cursor.return_value.__enter__.return_value.executemany.call_args
            self.assertIsNotNone(insert)
            query, records = insert.args
            self.assertEqual(query.count('%s'), len(records[0]))
            if kind == 'book':
                self.assertEqual((data['currently_reading'], data['want_to_read']), (4, 37))

    async def test_wrapped_http_error_is_preserved(self):
        response = SimpleNamespace(status=202, headers={'x-amzn-waf-action': 'challenge'})
        async def text(**kwargs):
            return (FIXTURES / 'challenge.html').read_text()
        response.text = text
        @asynccontextmanager
        async def get(url, **kwargs):
            yield response
        client = Client(delay=0)
        client.session = SimpleNamespace(get=get)
        with self.assertRaises(ResponseError):
            await client.fetch('book', '123')
        self.assertTrue(client.blocked)

    async def test_runner_stops_and_checkpoints_on_challenge(self):
        class BlockedClient:
            calls = []
            def __init__(self, **kwargs):
                self.blocked = None
            async def __aenter__(self):
                return self
            async def __aexit__(self, *exc):
                pass
            async def fetch(self, kind, identifier, **kwargs):
                self.calls.append(identifier)
                self.blocked = ResponseError(202, True)
                raise self.blocked
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'checkpoint.json'
            args = parser().parse_args(['collect', '--kind', 'book', '--ids', '123', '--fetch-only',
                                        '--batch-size', '2', '--batches', '2', '--checkpoint', str(path), '--attempts', '3'])
            state = {'version': 1, 'kind': 'book', 'similar': False, 'write': False,
                     'ids': ['123', '456', '789'], 'results': {}}
            conn = MagicMock()
            code = await run(conn, args, state, client_factory=BlockedClient)
            self.assertEqual(code, 2)
            self.assertEqual(BlockedClient.calls, ['123'])
            self.assertEqual(pending(json.loads(path.read_text())), ['123', '456', '789'])
            conn.execute.assert_not_called()


if __name__ == '__main__':
    unittest.main()
