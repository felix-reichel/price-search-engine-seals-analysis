import unittest
import datetime as dt
import polars as pl
from unittest.mock import patch

from impl.static import get_rand_max_20_counterfactual_firms


class TestGetRandMax20CounterfactualFirms(unittest.TestCase):

    def setUp(self):
        # Mock data setup
        self.product_id = 'product1'
        self.seal_date = dt.datetime(2022, 1, 17)
        self.seal_firms = ['firm1', 'firm2']

        # Angebot data including several firms, some of which are not seal_firms
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

    def test_counterfactual_firms_within_20(self):
        # Test where the number of counterfactual firms is less than or equal to 20
        result = get_rand_max_20_counterfactual_firms(
            self.product_id,
            self.seal_date,
            self.angebot_data,
            self.seal_firms
        )
        expected_firms = [
            'firm3', 'firm4', 'firm5', 'firm6', 'firm7', 'firm8', 'firm9', 'firm10',
            'firm11', 'firm12', 'firm13', 'firm14', 'firm15', 'firm16', 'firm17', 'firm18',
            'firm19', 'firm20', 'firm21', 'firm22', 'firm23', 'firm24', 'firm25', 'firm26',
            'firm27', 'firm28'
        ]
        # Ensure the result is a subset of the expected firms
        self.assertTrue(set(result).issubset(set(expected_firms)))
        # Ensure it doesn't include any seal firms
        self.assertTrue(all(firm not in self.seal_firms for firm in result))

    @patch('random.sample')
    def test_counterfactual_firms_more_than_20(self, mock_random_sample):
        # Mocking random.sample to return the first 20 firms for predictability
        mock_random_sample.return_value = [
            'firm3', 'firm4', 'firm5', 'firm6', 'firm7', 'firm8', 'firm9', 'firm10',
            'firm11', 'firm12', 'firm13', 'firm14', 'firm15', 'firm16', 'firm17', 'firm18',
            'firm19', 'firm20', 'firm21', 'firm22'
        ]
        result = get_rand_max_20_counterfactual_firms(
            self.product_id,
            self.seal_date,
            self.angebot_data,
            self.seal_firms
        )

        # Get the list of expected firms
        expected_firms = [
            'firm3', 'firm4', 'firm5', 'firm6', 'firm7', 'firm8', 'firm9', 'firm10',
            'firm11', 'firm12', 'firm13', 'firm14', 'firm15', 'firm16', 'firm17', 'firm18',
            'firm19', 'firm20', 'firm21', 'firm22', 'firm23', 'firm24', 'firm25', 'firm26',
            'firm27', 'firm28'
        ]

        # Ensure random.sample was called with the correct elements (order doesn't matter)
        args, kwargs = mock_random_sample.call_args
        self.assertCountEqual(args[0], expected_firms)  # Check elements without considering order
        self.assertEqual(args[1], 20)  # Ensure it sampled 20 elements

        # Ensure the result is exactly 20 firms
        self.assertEqual(len(result), 20)
        # Ensure it doesn't include any seal firms
        self.assertTrue(all(firm not in self.seal_firms for firm in result))


if __name__ == '__main__':
    unittest.main()
