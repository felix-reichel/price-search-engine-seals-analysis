import unittest
import datetime as dt
import polars as pl
from unittest.mock import patch

from impl.static import get_rand_max_N_counterfactual_firms


class TestGetRandMaxNCounterfactualFirms(unittest.TestCase):

    def setUp(self):
        # Setup common data for tests
        self.product_id = 'product1'
        self.seal_date = "17.1.2022"
        self.seal_firms = ['firm1', 'firm2']
        self.allowed_firms = [
            'firm3', 'firm4', 'firm5', 'firm6', 'firm7', 'firm8', 'firm9', 'firm10',
            'firm11', 'firm12', 'firm13', 'firm14', 'firm15', 'firm16', 'firm17', 'firm18',
            'firm19', 'firm20', 'firm21', 'firm22', 'firm23', 'firm24', 'firm25', 'firm26',
            'firm27', 'firm28'
        ]

        # Angebot data for firms
        self.angebot_data = pl.DataFrame({
            'produkt_id': ['product1'] * 30,
            'haendler_bez': [
                'firm1', 'firm1', 'firm2', 'firm3', 'firm3',
                'firm4', 'firm5', 'firm6', 'firm7', 'firm8',
                'firm9', 'firm10', 'firm11', 'firm12', 'firm13',
                'firm14', 'firm15', 'firm16', 'firm17', 'firm18',
                'firm19', 'firm20', 'firm21', 'firm22', 'firm23',
                'firm24', 'firm25', 'firm26', 'firm27', 'firm28'
            ],
            'dtimebegin': [dt.datetime(2022, 1, 10)] * 30,
            'dtimeend': [dt.datetime(2022, 1, 20)] * 30
        })

        self.expected_firms = {'firm3', 'firm4', 'firm5', 'firm6', 'firm7', 'firm8', 'firm9', 'firm10', 'firm11',
                               'firm12', 'firm13', 'firm14', 'firm15', 'firm16', 'firm17', 'firm18', 'firm19', 'firm20',
                               'firm21', 'firm22', 'firm23', 'firm24', 'firm25', 'firm26', 'firm27', 'firm28'}

    def assert_firms_are_valid(self, result):
        """
        Helper method to assert that the result contains valid firms.
        Ensures that:
        - The result firms are in the expected firms.
        - The result firms do not include any seal firms.
        """
        result_set = set(result)
        self.assertTrue(result_set.issubset(self.expected_firms), "Result contains firms not in expected firms.")
        self.assertTrue(all(firm not in self.seal_firms for firm in result), "Result contains seal firms.")

    def test_counterfactual_firms_within_N(self):
        # Test where the number of counterfactual firms is less than or equal to N (10 in this case)
        result = get_rand_max_N_counterfactual_firms(
            self.product_id,
            self.seal_date,
            self.angebot_data,
            self.seal_firms,
            self.allowed_firms
        )

        # Assert that firms are valid and within the allowed subset
        self.assert_firms_are_valid(result)
        # Ensure the result contains at most 10 firms
        self.assertLessEqual(len(result), 10)

    @patch('random.sample')
    def test_counterfactual_firms_more_than_N(self, mock_random_sample):
        # Mocking random.sample to return 10 firms for predictability
        mock_random_sample.return_value = [
            'firm3', 'firm4', 'firm5', 'firm6', 'firm7', 'firm8', 'firm9', 'firm10',
            'firm11', 'firm12'
        ]

        result = get_rand_max_N_counterfactual_firms(
            self.product_id,
            self.seal_date,
            self.angebot_data,
            self.seal_firms,
            self.allowed_firms
        )

        # Ensure random.sample was called with the correct elements
        args, kwargs = mock_random_sample.call_args
        self.assertTrue(set(args[0]).issubset(self.expected_firms))  # Check that the sample set is valid
        self.assertEqual(args[1], 10)  # Ensure it sampled 10 elements

        # Assert that firms are valid and within the allowed subset
        self.assert_firms_are_valid(result)
        # Ensure the result contains exactly 10 firms
        self.assertEqual(len(result), 10)


if __name__ == '__main__':
    unittest.main()
