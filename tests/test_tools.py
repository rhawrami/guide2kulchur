import asyncio
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

from scripts.analysis.demographics import render, summarize, main as demographics_main
from scripts.analysis.export_maps import query_for
from scripts.analysis.networks import graph_from_rows, main as networks_main
from scripts.collection.discover import locations, sitemap_url, main as discovery_main
from scripts.enrichment.geocode import geocode, main as geocode_main
from scripts.enrichment.import_awards import records, main as awards_main
from scripts.enrichment.wikidata import convert, pg_date, main as wikidata_main
from scripts.maintenance.inspect_db import main as inspect_main
from guide2kulchur.pipeline.cli import parser
from guide2kulchur.pipeline.runner import pending, run


class ToolTests(unittest.TestCase):
    def test_all_help_paths_are_offline(self):
        for entry in [demographics_main, networks_main, discovery_main, geocode_main, awards_main, wikidata_main, inspect_main]:
            with self.subTest(entry=entry.__module__), redirect_stdout(io.StringIO()), self.assertRaises(SystemExit) as caught:
                entry(['--help'])
            self.assertEqual(caught.exception.code, 0)

    def test_sitemap_parsing_and_host_validation(self):
        xml = '<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9"><url><loc>https://www.goodreads.com/author/show/42</loc></url></urlset>'
        self.assertEqual(locations(xml), ['https://www.goodreads.com/author/show/42'])
        self.assertEqual(sitemap_url('https://www.goodreads.com/sitemap.1.xml.gz'), 'https://www.goodreads.com/sitemap.1.xml.gz')
        with self.assertRaises(ValueError):
            sitemap_url('https://example.invalid/sitemap.xml')

    def test_discovery_preview_is_offline_and_writes_nothing(self):
        with patch('scripts.collection.discover.discover') as live, redirect_stdout(io.StringIO()):
            self.assertEqual(discovery_main(['genres', '--genre', 'fiction', '--output', '/unused/seeds.json']), 0)
            live.assert_not_called()

    def test_wikidata_dates_and_mapping(self):
        self.assertEqual(pg_date('-0044-03-15T00:00:00Z'), '0044-03-15 BC')
        self.assertEqual(pg_date('0000-01-01T00:00:00Z'), '0001-01-01')
        self.assertIsNone(pg_date(None))
        self.assertEqual(convert({'author': {'value': 'Q1'}}), ('Q1', None, None, None, None, None, None))

    def test_wikidata_preview_is_offline(self):
        with patch('scripts.enrichment.wikidata.requests.get') as request, redirect_stdout(io.StringIO()):
            self.assertEqual(wikidata_main(['--start-year', '1000', '--end-year', '1900', '--limit', '25']), 0)
            request.assert_not_called()

    def test_awards_deduplicate_and_preview_without_database(self):
        row = {'id': '123', 'author_id': '42', 'award_year': 2025, 'award_category': 'Fiction', 'award_num_votes': 100}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'awards.json'
            path.write_text(json.dumps({'results': [row, row]}))
            self.assertEqual(len(list(records([path, path]))), 1)
            with patch('scripts.enrichment.import_awards.database') as db, redirect_stdout(io.StringIO()):
                self.assertEqual(awards_main(['--input', str(path)]), 0)
                db.assert_not_called()

    def test_bounded_graph_excludes_missing_neighbors(self):
        graph = graph_from_rows([('123', 'Example Book', 10, ['456', '999', '123']), ('456', 'Other Book', 20, [])])
        self.assertEqual(set(graph.nodes), {'123', '456'})
        self.assertEqual(set(graph.edges), {('123', '456')})

    def test_demographic_empty_and_zero_counts(self):
        self.assertEqual(summarize([('empty', None, None)]), [])
        self.assertEqual(summarize([('fiction', 1, 3)]), [('fiction', 1, 3, 4, 25, 75)])
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'plot.html'
            render(summarize([('fiction', 1, 3)]), 'Synthetic estimates', path)
            self.assertTrue(path.exists())

    def test_map_queries_are_select_only(self):
        for kind in ('book', 'author'):
            query = query_for(kind).as_string()
            self.assertTrue(query.startswith('SELECT '))
            self.assertIn('LIMIT %s', query)


class AsyncToolTests(unittest.IsolatedAsyncioTestCase):
    async def test_geocoder_fetch_only_does_not_write(self):
        locator = MagicMock()
        locator.geocode = AsyncMock(return_value=SimpleNamespace(address='Example Place', latitude=1, longitude=2))
        locator.__aenter__ = AsyncMock(return_value=locator)
        locator.__aexit__ = AsyncMock(return_value=None)
        args = SimpleNamespace(user_agent='test', timeout=5, request_delay=1, batch_size=1, write=False)
        conn = MagicMock()
        with patch('scripts.enrichment.geocode.Nominatim', return_value=locator), patch('scripts.enrichment.geocode.AsyncRateLimiter', side_effect=lambda fn, **kwargs: fn), redirect_stdout(io.StringIO()):
            await geocode(conn, ['example place'], args)
        conn.transaction.assert_not_called()
        self.assertEqual(locator.geocode.await_count, 1)

    async def test_filtered_records_are_not_persisted(self):
        class Client:
            def __init__(self, **kwargs):
                self.blocked = None
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            async def fetch(self, *args, **kwargs):
                return {'rating_count': 0, 'review_count': 0}
        with tempfile.TemporaryDirectory() as tmp:
            args = parser().parse_args(['collect', '--kind', 'book', '--ids', '123', '--fetch-only',
                                        '--min-ratings', '100', '--checkpoint', str(Path(tmp) / 'checkpoint.json')])
            state = {'version': 1, 'kind': 'book', 'similar': False, 'write': False, 'ids': ['123'], 'results': {}}
            with redirect_stdout(io.StringIO()):
                self.assertEqual(await run(MagicMock(), args, state, client_factory=Client), 0)
            self.assertEqual(state['results']['123']['status'], 'filtered')
            self.assertEqual(pending(state), [])


if __name__ == '__main__':
    unittest.main()
