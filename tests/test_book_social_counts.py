import json
import unittest

from bs4 import BeautifulSoup

from guide2kulchur.engineer.recruits import HouseOfWisdom
from guide2kulchur.privateer.alexandria import Alexandria


def scraper(html, cls=Alexandria):
    book = cls()
    book._soup = BeautifulSoup(html, 'lxml')
    return book


def embedded(signals, **extra_state):
    state = {
        'ROOT_QUERY': {'getSocialSignals({"bookId":"test"})': signals},
        **extra_state,
    }
    data = {'props': {'pageProps': {'apolloState': state}}}
    return f'<script id="__NEXT_DATA__" type="application/json">{json.dumps(data)}</script>'


def counts(book):
    return book.get_currently_reading(), book.get_want_to_read()


class BookSocialCountsTests(unittest.TestCase):
    def test_legacy_html(self):
        book = scraper('''
            <div data-testid="currentlyReadingSignal"><span>1,234</span> people are currently reading</div>
            <div data-testid="toReadSignal">1 person wants to read</div>
        ''')
        self.assertEqual(counts(book), (1234, 1))

    def test_embedded_counts_with_loading_placeholders(self):
        html = '<div class="SocialSignalsSection__loading">Loading...</div>' + embedded([
            {'name': 'CURRENTLY_READING', 'count': 4},
            {'name': 'TO_READ', 'count': 37},
        ])
        for cls in (Alexandria, HouseOfWisdom):
            with self.subTest(cls=cls.__name__):
                self.assertEqual(counts(scraper(html, cls)), (4, 37))

    def test_zero_is_a_valid_count(self):
        book = scraper(embedded([
            {'name': 'CURRENTLY_READING', 'count': 0},
            {'name': 'TO_READ', 'count': 0},
        ]))
        self.assertEqual(counts(book), (0, 0))
        self.assertEqual(scraper('<div data-testid="toReadSignal">0 people want to read</div>').get_want_to_read(), 0)

    def test_legacy_html_takes_precedence(self):
        html = '<div data-testid="toReadSignal">12 people want to read</div>'
        html += embedded([{'name': 'TO_READ', 'count': 37}])
        self.assertEqual(scraper(html).get_want_to_read(), 12)

    def test_invalid_legacy_text_falls_back_to_json(self):
        for text in ('Loading...', '1.2k people want to read', '-1 person', ''):
            with self.subTest(text=text):
                html = f'<div data-testid="toReadSignal">{text}</div>'
                html += embedded([{'name': 'TO_READ', 'count': 37}])
                self.assertEqual(scraper(html).get_want_to_read(), 37)

    def test_missing_or_invalid_json_returns_none(self):
        for raw in ('', '{', 'null', '[]', '42', '{}', '{"props": null}',
                    '{"props":{"pageProps":{"apolloState":{"ROOT_QUERY":[]}}}}'):
            with self.subTest(raw=raw):
                self.assertEqual(counts(scraper(f'<script id="__NEXT_DATA__">{raw}</script>')), (None, None))
        self.assertEqual(counts(scraper('<html></html>')), (None, None))

    def test_invalid_signal_counts_return_none(self):
        for value in (None, -1, True, False, '37', 1.5, [], {}):
            with self.subTest(value=value):
                book = scraper(embedded([{'name': 'TO_READ', 'count': value}]))
                self.assertIsNone(book.get_want_to_read())

    def test_missing_signals_and_unrelated_queries(self):
        for signals in (None, {}, [], [None, 1, 'bad', {}, {'name': 'OTHER', 'count': 99}]):
            with self.subTest(signals=signals):
                self.assertEqual(counts(scraper(embedded(signals))), (None, None))
        self.assertEqual(counts(scraper(embedded([], ROOT_QUERY={'otherQuery': [{'name': 'TO_READ', 'count': 99}]}))), (None, None))

    def test_one_missing_signal_does_not_hide_the_other(self):
        book = scraper(embedded([None, {'name': 'TO_READ', 'count': 37}]))
        self.assertEqual(counts(book), (None, 37))

    def test_apollo_references(self):
        book = scraper(embedded(
            [{'__ref': 'missing'}, {'__ref': 'SocialSignal:TO_READ'}],
            **{'SocialSignal:TO_READ': {'name': 'TO_READ', 'count': 37}},
        ))
        self.assertEqual(counts(book), (None, 37))

    def test_unloaded_book_still_raises(self):
        with self.assertRaises(RuntimeError):
            Alexandria().get_currently_reading()
        with self.assertRaises(RuntimeError):
            Alexandria().get_want_to_read()


if __name__ == '__main__':
    unittest.main()
