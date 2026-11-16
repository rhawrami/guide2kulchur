from contextlib import redirect_stderr, redirect_stdout
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import MagicMock, patch

from guide2kulchur.pipeline.activity import activity_policy
from guide2kulchur.pipeline.cli import main, parser
from guide2kulchur.pipeline.common import atomic_json
from guide2kulchur.pipeline.runner import run


class ActivityTests(unittest.TestCase):
    def test_users_require_a_rating_by_default(self):
        policy = activity_policy('user')
        self.assertEqual((policy.min_ratings, policy.min_reviews, policy.mode), (1, 0, 'all'))
        for ratings, reviews, expected in [(0, 0, False), (0, 10, False), (1, 0, True), (1, 1, True)]:
            with self.subTest(ratings=ratings, reviews=reviews):
                self.assertEqual(policy.accepts({'rating_count': ratings, 'review_count': reviews}), expected)

    def test_books_and_authors_require_a_rating_or_review(self):
        for kind in ('book', 'author'):
            policy = activity_policy(kind)
            self.assertEqual((policy.min_ratings, policy.min_reviews, policy.mode), (1, 1, 'any'))
            for ratings, reviews, expected in [(0, 0, False), (0, 1, True), (1, 0, True), (1, 1, True)]:
                with self.subTest(kind=kind, ratings=ratings, reviews=reviews):
                    self.assertEqual(policy.accepts({'rating_count': ratings, 'review_count': reviews}), expected)

    def test_missing_or_invalid_counts_do_not_prove_activity(self):
        for kind in ('book', 'author', 'user'):
            for value in (None, -1, True, False, '1', [], 1.5):
                with self.subTest(kind=kind, value=value):
                    self.assertFalse(activity_policy(kind).accepts({'rating_count': value, 'review_count': value}))
            self.assertFalse(activity_policy(kind).accepts({}))
        self.assertTrue(activity_policy('book').accepts({'rating_count': None, 'review_count': 1}))

    def test_custom_any_and_all_policies(self):
        some_activity = {'rating_count': 100, 'review_count': 0}
        self.assertTrue(activity_policy('book', 100, 10, 'any').accepts(some_activity))
        self.assertFalse(activity_policy('book', 100, 10, 'all').accepts(some_activity))
        self.assertTrue(activity_policy('book', 100, 10, 'all').accepts({'rating_count': 100, 'review_count': 10}))

    def test_zero_disables_a_threshold_not_the_any_gate(self):
        self.assertFalse(activity_policy('book', 100, 0, 'any').accepts({'rating_count': 0, 'review_count': 10}))
        self.assertTrue(activity_policy('book', 100, 0, 'any').accepts({'rating_count': 100, 'review_count': 0}))
        self.assertFalse(activity_policy('user', 0, 10, 'any').accepts({'rating_count': 0, 'review_count': 0}))
        self.assertTrue(activity_policy('user', 0, 10, 'any').accepts({'rating_count': 0, 'review_count': 10}))
        for mode in ('any', 'all'):
            self.assertTrue(activity_policy('book', 0, 0, mode).accepts({}))

    def test_cli_preview_reports_effective_defaults(self):
        for kind, reviews, mode in [('book', 1, 'any'), ('author', 1, 'any'), ('user', 0, 'all')]:
            output = io.StringIO()
            with patch('guide2kulchur.pipeline.cli.database'), patch('guide2kulchur.pipeline.cli.missing_ids', return_value=[]), redirect_stdout(output):
                self.assertEqual(main(['collect', '--kind', kind, '--ids', '123']), 0)
            self.assertEqual(json.loads(output.getvalue())['activity'], {'min_ratings': 1, 'min_reviews': reviews, 'policy': mode})

    def test_invalid_cli_policy_never_connects(self):
        for flags in [['--min-ratings', '-1'], ['--min-reviews', '-1'], ['--activity-policy', 'invalid']]:
            with self.subTest(flags=flags), patch('guide2kulchur.pipeline.cli.database') as db, redirect_stderr(io.StringIO()):
                with self.assertRaises(SystemExit):
                    main(['collect', '--kind', 'book', '--ids', '123', *flags])
                db.assert_not_called()

    def test_similar_is_not_subject_to_entity_defaults(self):
        with patch('guide2kulchur.pipeline.cli.database'), patch('guide2kulchur.pipeline.cli.similar_ids', return_value=[]), redirect_stdout(io.StringIO()):
            self.assertEqual(main(['similar', '--kind', 'book']), 0)
        with patch('guide2kulchur.pipeline.cli.database') as db, redirect_stderr(io.StringIO()), self.assertRaises(SystemExit):
            main(['similar', '--kind', 'book', '--activity-policy', 'any'])
        db.assert_not_called()

    def test_resume_keeps_the_original_policy(self):
        state = {'version': 1, 'kind': 'book', 'similar': False, 'write': False,
                 'min_ratings': 1, 'min_reviews': 1, 'activity_policy': 'all',
                 'ids': ['123'], 'results': {'123': {'status': 'success'}}}
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / 'checkpoint.json'
            atomic_json(path, state)
            with patch('guide2kulchur.pipeline.cli.database') as db, redirect_stderr(io.StringIO()):
                self.assertEqual(main(['collect', '--kind', 'book', '--resume', str(path)]), 1)
                db.assert_not_called()
            with patch('guide2kulchur.pipeline.cli.database'), redirect_stdout(io.StringIO()):
                self.assertEqual(main(['collect', '--kind', 'book', '--resume', str(path), '--activity-policy', 'all']), 0)
            del state['activity_policy']  # Original checkpoints always used AND.
            atomic_json(path, state)
            with patch('guide2kulchur.pipeline.cli.database'), redirect_stdout(io.StringIO()):
                self.assertEqual(main(['collect', '--kind', 'book', '--resume', str(path), '--activity-policy', 'all']), 0)


class ActivityRunnerTests(unittest.IsolatedAsyncioTestCase):
    async def test_unrated_records_never_reach_inserts_or_error_ids(self):
        class UnratedClient:
            def __init__(self, **kwargs):
                self.blocked = None
            async def __aenter__(self):
                return self
            async def __aexit__(self, *args):
                pass
            async def fetch(self, *args, **kwargs):
                return {'rating_count': 0, 'review_count': 0}

        for kind in ('book', 'author', 'user'):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as tmp:
                path = Path(tmp) / 'checkpoint.json'
                args = parser().parse_args(['collect', '--kind', kind, '--ids', '123', '--write', '--checkpoint', str(path)])
                state = {'version': 1, 'kind': kind, 'similar': False, 'write': True, 'ids': ['123'], 'results': {}}
                conn = MagicMock()
                with redirect_stdout(io.StringIO()):
                    self.assertEqual(await run(conn, args, state, client_factory=UnratedClient), 0)
                self.assertEqual(state['results']['123']['status'], 'filtered')
                conn.execute.assert_not_called()
                conn.cursor.assert_not_called()


if __name__ == '__main__':
    unittest.main()
