from veetility.utility_functions import UtilityFunctions
import unittest
import os


class TestBestFuzzyMatch(unittest.TestCase):

    def setUp(self):
        self.util_class = UtilityFunctions()

    def test_best_fuzzy_match_exact_match(self):
        list_1 = ["apple", "banana", "cherry"]
        list_2 = ["apple", "banana", "cherry"]
        threshold = 80
        json_name = "test_exact_match"

        expected_result = {"apple": "apple", "banana": "banana", "cherry": "cherry"}

        result = self.util_class.best_fuzzy_match(list_1, list_2, threshold, json_name)
        self.assertEqual(result, expected_result)

        # Clean up the created JSON file
        os.remove(f"JSON Files/best_match_dict_{json_name}.json")

    def test_best_fuzzy_match_no_match(self):
        list_1 = ["apple", "banana", "cherry"]
        list_2 = ["dog", "cat", "fish"]
        threshold = 80
        json_name = "test_no_match"

        expected_result = {"apple": "None", "banana": "None", "cherry": "None"}

        result = self.util_class.best_fuzzy_match(list_1, list_2, threshold, json_name)
        self.assertEqual(result, expected_result)

        # Clean up the created JSON file
        os.remove(f"JSON Files/best_match_dict_{json_name}.json")

    def test_best_fuzzy_match_null_values(self):
        list_1 = ["apple", "banana", "cherry", 'nan', 'None']
        list_2 = ["dog", "cat", "fish", 'nan', 'None']
        threshold = 80
        json_name = "test_null_values"

        expected_result = {"apple": "None", "banana": "None", "cherry": "None", "nan": "nan", "None": "None"}

        result = self.util_class.best_fuzzy_match(list_1, list_2, threshold, json_name)
        self.assertEqual(result, expected_result)

        # Clean up the created JSON file
        os.remove(f"JSON Files/best_match_dict_{json_name}.json")


if __name__ == '__main__':
    unittest.main()