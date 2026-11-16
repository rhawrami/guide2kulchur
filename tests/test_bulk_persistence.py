from contextlib import contextmanager, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock

import psycopg

from guide2kulchur.pipeline.cli import parser
from guide2kulchur.pipeline.runner import pending, persist, run

FIELDS = {
    'book': 'id title author author_id isbn language description image_url rating rating_distribution rating_count review_count top_genres currently_reading want_to_read first_published page_length similar_books_id',
    'author': 'author_id author_name description image_url birth_place birth death top_genres influences book_sample quotes_sample rating rating_count review_count follower_count',
    'user': 'user_id user_name image_url rating rating_count review_count favorite_genres follower_count friend_count currently_reading_sample_books currently_reading_sample_authors featured_shelf_sample_books shelves followings_sample_users followings_sample_authors quotes_sample_strings quotes_sample_author_ids friends_sample currently_reading_update_time',
}


def record(identifier, kind='book'):
    data = dict.fromkeys(FIELDS[kind].split())
    id_key, name_key = {'book': ('id', 'title'), 'author': ('author_id', 'author_name'), 'user': ('user_id', 'user_name')}[kind]
    data.update({id_key: identifier, name_key: 'Synthetic example', 'rating_count': 1, 'review_count': 0})
    if kind == 'book':
        data['author'] = 'Synthetic author'
    return {'id': identifier, 'status': 'success', 'data': data}


class MemoryConnection:
    """Small transactional fake, including rollback of partially submitted batches."""
    def __init__(self, bad_ids=(), connection_error=False):
        self.rows = {}
        self.bad_ids = set(bad_ids)
        self.connection_error = connection_error
        self.depth = self.commits = self.savepoints = self.rollbacks = 0
        self.calls = []

    @contextmanager
    def transaction(self):
        before = self.rows.copy()
        outer = self.depth == 0
        if not outer:
            self.savepoints += 1
        self.depth += 1
        try:
            yield
        except BaseException:
            self.rows = before
            self.rollbacks += 1
            raise
        else:
            if outer:
                self.commits += 1
        finally:
            self.depth -= 1

    @contextmanager
    def cursor(self):
        yield self

    def executemany(self, query, rows):
        rows = list(rows)
        self.calls.append({'query': query, 'rows': rows, 'existing': set(self.rows)})
        if self.connection_error:
            raise psycopg.OperationalError('Synthetic disconnected database')
        for row in rows:
            if row[0] in self.bad_ids:
                raise psycopg.DataError('Synthetic invalid value')
            self.rows.setdefault(row[0], row)  # Simulate ON CONFLICT DO NOTHING.


class BulkPersistenceTests(unittest.TestCase):
    def test_one_bulk_call_and_one_commit_for_every_entity_kind(self):
        for kind in FIELDS:
            with self.subTest(kind=kind):
                conn = MemoryConnection()
                persist(conn, kind, [record('1', kind), record('2', kind), record('3', kind)])
                self.assertEqual(len(conn.calls), 1)
                self.assertEqual(len(conn.calls[0]['rows']), 3)
                self.assertIn('ON CONFLICT DO NOTHING', conn.calls[0]['query'])
                self.assertEqual((conn.commits, conn.savepoints, conn.rollbacks), (1, 1, 0))
                self.assertEqual(set(conn.rows), {'1', '2', '3'})

    def test_bulk_failure_rolls_back_before_per_row_recovery(self):
        conn = MemoryConnection(bad_ids={'2'})
        records = [record('1'), record('2'), record('3')]
        with self.assertLogs('pipeline.insert', level='WARNING'):
            persist(conn, 'book', records)
        self.assertEqual([len(call['rows']) for call in conn.calls], [3, 1, 1, 1])
        self.assertEqual(conn.calls[1]['existing'], set())
        self.assertEqual(set(conn.rows), {'1', '3'})
        self.assertEqual([row['status'] for row in records], ['success', 'db_error', 'success'])
        self.assertEqual(records[1]['error'], 'DataError')
        self.assertEqual(conn.commits, 1)
        self.assertEqual(conn.rollbacks, 2)  # Bulk attempt and the bad singleton.

    def test_tuple_preparation_failure_also_isolates_bad_record(self):
        conn = MemoryConnection()
        records = [record('1'), record('2'), record('3')]
        del records[1]['data']['title']
        with self.assertLogs('pipeline.insert', level='WARNING'):
            persist(conn, 'book', records)
        self.assertEqual(set(conn.rows), {'1', '3'})
        self.assertEqual(records[1]['status'], 'db_error')
        self.assertEqual(records[1]['error'], 'KeyError')
        self.assertEqual(len(conn.calls), 2)  # No SQL submitted for the unprepared batch/bad row.

    def test_duplicate_ids_are_submitted_once(self):
        conn = MemoryConnection()
        records = [record('1'), record('1'), record('2')]
        persist(conn, 'book', records)
        self.assertEqual([row[0] for row in conn.calls[0]['rows']], ['1', '2'])
        self.assertEqual([row['status'] for row in records], ['success'] * 3)

    def test_duplicate_failures_have_consistent_checkpoint_outcomes(self):
        conn = MemoryConnection(bad_ids={'1'})
        records = [record('1'), record('1')]
        with self.assertLogs('pipeline.insert', level='WARNING'):
            persist(conn, 'book', records)
        self.assertEqual(len(conn.calls), 2)
        self.assertTrue(all(row['status'] == 'db_error' for row in records))

    def test_successful_duplicate_overrides_a_conflicting_missing_outcome(self):
        conn = MemoryConnection()
        records = [{'id': '1', 'status': 'missing'}, record('1')]
        persist(conn, 'book', records)
        self.assertEqual(len(conn.calls), 1)
        self.assertIn('INSERT INTO alexandria', conn.calls[0]['query'])
        self.assertTrue(all(row['status'] == 'success' for row in records))

    def test_existing_ids_do_not_fail_or_overwrite(self):
        conn = MemoryConnection()
        conn.rows['1'] = ('1', 'Existing title')
        persist(conn, 'book', [record('1'), record('2')])
        self.assertEqual(conn.rows['1'], ('1', 'Existing title'))
        self.assertEqual(len(conn.calls), 1)
        self.assertEqual(conn.rollbacks, 0)

    def test_missing_ids_are_batched_separately_with_same_commit(self):
        conn = MemoryConnection()
        records = [record('1'), {'id': '2', 'status': 'missing'}, {'id': '3', 'status': 'missing'}]
        persist(conn, 'book', records)
        self.assertEqual([len(call['rows']) for call in conn.calls], [1, 2])
        self.assertIn('public.error_id', conn.calls[1]['query'])
        self.assertIn('ON CONFLICT DO NOTHING', conn.calls[1]['query'])
        self.assertEqual(conn.commits, 1)

    def test_similar_updates_are_also_batched(self):
        for kind, column in [('book', 'sim_books'), ('author', 'sim_authors')]:
            with self.subTest(kind=kind):
                conn = MagicMock()
                persist(conn, kind, [{'id': '1', 'status': 'success', 'data': ['2']},
                                     {'id': '3', 'status': 'success', 'data': ['4']}], similar=True)
                insert = conn.cursor.return_value.__enter__.return_value.executemany
                insert.assert_called_once()
                self.assertIn(f'SET {column} = %s', insert.call_args.args[0])
                self.assertEqual(insert.call_args.args[1], [(['2'], '1'), (['4'], '3')])
                self.assertEqual(conn.transaction.call_count, 2)

    def test_nonwritable_outcomes_do_not_start_a_transaction(self):
        conn = MagicMock()
        persist(conn, 'book', [{'id': '1', 'status': 'filtered'}, {'id': '2', 'status': 'blocked'}])
        conn.transaction.assert_not_called()
        conn.cursor.assert_not_called()

    def test_pending_deduplicates_checkpoint_ids(self):
        self.assertEqual(pending({'ids': ['1', '1', '2'], 'results': {}}), ['1', '2'])

    def test_connection_failure_is_not_retried_as_bad_rows(self):
        conn = MemoryConnection(connection_error=True)
        with self.assertRaises(psycopg.OperationalError):
            persist(conn, 'book', [record('1'), record('2')])
        self.assertEqual(len(conn.calls), 1)
        self.assertEqual(conn.commits, 0)
        self.assertEqual(conn.rows, {})


    def test_schema_failure_is_not_retried_as_bad_rows(self):
        conn = MemoryConnection()
        conn.executemany = MagicMock(side_effect=psycopg.errors.UndefinedColumn('Synthetic schema mismatch'))
        with self.assertRaises(psycopg.errors.UndefinedColumn):
            persist(conn, 'book', [record('1'), record('2')])
        conn.executemany.assert_called_once()
        self.assertEqual(conn.commits, 0)


class CheckpointTests(unittest.IsolatedAsyncioTestCase):
    async def test_no_success_checkpoint_on_failed_batch_commit(self):
        class Client:
            def __init__(self, **kwargs):
                self.blocked = None
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            async def fetch(self, kind, identifier, **kwargs):
                return record(identifier)['data']

        class CommitFails(MemoryConnection):
            @contextmanager
            def transaction(self):
                outer = self.depth == 0
                with super().transaction():
                    yield
                    if outer:
                        raise psycopg.OperationalError('Synthetic commit failure')

        for conn in (MemoryConnection(connection_error=True), CommitFails()):
            with self.subTest(connection=type(conn).__name__), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / 'checkpoint.json'
                args = parser().parse_args(['collect', '--kind', 'book', '--ids', '1', '--write', '--checkpoint', str(path)])
                state = {'version': 1, 'kind': 'book', 'similar': False, 'write': True, 'ids': ['1'], 'results': {}}
                with redirect_stdout(io.StringIO()), self.assertRaises(psycopg.OperationalError):
                    await run(conn, args, state, client_factory=Client)
                saved = json.loads(path.read_text())
                self.assertEqual(saved['results'], {})
                self.assertEqual(pending(saved), ['1'])
                self.assertEqual(conn.rows, {})
                self.assertEqual(conn.commits, 0)


if __name__ == '__main__':
    unittest.main()
