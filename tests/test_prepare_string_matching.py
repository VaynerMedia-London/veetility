from veetility.utility_functions import UtilityFunctions
import unittest
import pandas as pd


class TestPrepareStringMatching(unittest.TestCase):

    def setUp(self):
        self.util_class = UtilityFunctions()

    def test_empty_string(self):
        self.assertEqual(self.util_class.prepare_string_matching(''), '')
    
    def test_nan_string(self):
        self.assertTrue(pd.isna(self.util_class.prepare_string_matching(pd.NA)))

    def test_whitespace_removal(self):
        self.assertEqual(self.util_class.prepare_string_matching('   Test String   '), 'teststring')
    
    def test_utm_removal(self):
        """If the string is a URL, remove the UTM parameters."""
        self.assertEqual(self.util_class.prepare_string_matching('https://example.com/cool-page.html?utm_source=google&utm_medium=cpc&utm_campaign=123', is_url=True), 'httpsexamplecomcoolpagehtml')

    def test_url_removal(self):
        """If the string is not a message that contains a URL, remove the URL."""
        self.assertEqual(self.util_class.prepare_string_matching('Check out this https://example.com/cool-page.html page!', is_url=False), 'checkoutthispage')

    def test_emoji_removal(self):
        self.assertEqual(self.util_class.prepare_string_matching('Hello 😊'), 'hello')

    def test_non_ascii_removal(self):
        self.assertEqual(self.util_class.prepare_string_matching('café'), 'cafe')

    def test_punctuation_removal(self):
        self.assertEqual(self.util_class.prepare_string_matching('Hello, World!$%'), 'helloworld')


if __name__ == '__main__':
    unittest.main()