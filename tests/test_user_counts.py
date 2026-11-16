from pathlib import Path
import unittest

from bs4 import BeautifulSoup

from guide2kulchur.engineer.recruits import FalseBardiya
from guide2kulchur.privateer.recruits import _get_user_stat


class UserCountTests(unittest.TestCase):
    def test_grouped_ratings_and_reviews(self):
        self.assertEqual(_get_user_stat('1,234 ratings', 'num_ratings'), 1234)
        self.assertEqual(_get_user_stat('2,345 reviews', 'num_reviews'), 2345)
        self.assertEqual(_get_user_stat('(4.20 avg)', 'avg_ratings'), 4.2)
        self.assertIsNone(_get_user_stat('unknown ratings', 'num_ratings'))

    def test_pipeline_does_not_drop_active_users_with_grouped_counts(self):
        html = (Path(__file__).parent / 'fixtures/user.html').read_text()
        html = html.replace('123 ratings', '1,234 ratings').replace('12 reviews', '2,345 reviews')
        html = html.replace('<div class="rightContainer">', '<div class="rightContainer"><a class="actionLinkLite">1,234 people are following</a>')
        user = FalseBardiya()
        user.user_url = 'https://www.goodreads.com/user/show/7'
        user._soup = BeautifulSoup(html, 'lxml')
        user._info_main = user._soup.find('div', class_='mainContentFloat')
        user._info_left = user._info_main.find('div', class_='leftContainer')
        user._info_right = user._info_main.find('div', class_='rightContainer')
        data = user.get_all_data()
        self.assertEqual(data['rating_count'], 1234)
        self.assertEqual(data['review_count'], 2345)
        self.assertEqual(data['follower_count'], 1234)


if __name__ == '__main__':
    unittest.main()
