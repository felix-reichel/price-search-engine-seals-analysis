import unittest
import pandas as pd

from src.MatchingCriteria import MatchingCriteria


class MatchingCriteriaAdvancedTest(unittest.TestCase):

    def test_matching_criteria_advanced(self):
        filtered_retailers_df = pd.DataFrame({1: ['retailer-1', 'retailer-2', 'retailer-3', 'geizhals']})
        all_retailers = ['retailer-1', 'retailer-2', 'retailer-3']

        matches_1 = MatchingCriteria.matching_criteria_advanced("retailer-3", filtered_retailers_df)
        self.assertEqual(all_retailers, matches_1)

        matches_2 = MatchingCriteria.matching_criteria_advanced(
            "https://retailer-2.com/shop".lstrip("https://").rstrip(".com/shop"), filtered_retailers_df)
        self.assertEqual(all_retailers, matches_2)

        # Not sufficient case
        matches_3 = MatchingCriteria.matching_criteria_advanced(
            "https://tailer-2.com/shop".lstrip("https://").rstrip(".com/shop"), filtered_retailers_df)
        self.assertEqual(['retailer-2'], matches_3)

    def test_matching_criteria_advanced_case_geizhals(self):
        filtered_retailers_df = pd.DataFrame({1: ['retailer-1', 'retailer-2', 'retailer-3', 'geizhals']})

        gz = ['geizhals']
        matches_4 = MatchingCriteria.matching_criteria_advanced("https://geizh.com/shop",
                                                                filtered_retailers_df)

        matches_5 = MatchingCriteria.matching_criteria_advanced("zhals",
                                                                filtered_retailers_df)

        matches_6 = MatchingCriteria.matching_criteria_advanced("geizhals",
                                                                filtered_retailers_df)
        self.assertEqual(matches_4, gz)
        self.assertEqual(matches_5, gz)
        self.assertEqual(matches_6, gz)


if __name__ == '__main__':
    unittest.main()
