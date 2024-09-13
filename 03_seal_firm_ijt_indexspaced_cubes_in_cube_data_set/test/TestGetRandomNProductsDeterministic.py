import unittest
import datetime as dt
import polars as pl
from unittest.mock import patch
from impl.static import get_random_n_products_deterministic


class TestGetRandomNProductsDeterministic(unittest.TestCase):

    def setUp(self):
        # Setup common data for tests
        self.haendler_bez = 'firm1'
        self.seal_date = "17.1.2022"
        self.seed = 42
        self.n = 10  # Number of products to sample

        # Angebot data for the firm
        self.angebot_data = pl.DataFrame({
            'produkt_id': ['product1', 'product2', 'product3', 'product4', 'product5',
                           'product6', 'product7', 'product8', 'product9', 'product10',
                           'product11', 'product12', 'product13', 'product14', 'product15'],
            'haendler_bez': ['firm1'] * 15,  # All products offered by firm1
            'dtimebegin': [dt.datetime(2022, 1, 10)] * 15,
            'dtimeend': [dt.datetime(2022, 1, 20)] * 15
        })

    def test_random_n_products_within_N(self):
        # Test where the number of products is greater than or equal to N
        result = get_random_n_products_deterministic(
            self.haendler_bez,
            self.angebot_data,
            self.seal_date,
            n=self.n,
            seed=self.seed
        )

        # Ensure the result contains exactly N products
        self.assertEqual(len(result), self.n)
        # Ensure that all selected products are from the angebot_data product list
        product_ids = self.angebot_data['produkt_id'].to_list()
        self.assertTrue(set(result).issubset(product_ids), "Result contains products not in the offered products.")

    def test_random_n_products_less_than_N(self):
        # Test where the number of products is less than N (e.g., only 5 products available)
        limited_angebot_data = self.angebot_data.head(5)  # Only 5 products available

        result = get_random_n_products_deterministic(
            self.haendler_bez,
            limited_angebot_data,
            self.seal_date,
            n=self.n,
            seed=self.seed
        )

        # Ensure the result contains all the available products (5 in this case)
        self.assertEqual(len(result), 5)
        # Ensure that all selected products are from the available products
        product_ids = limited_angebot_data['produkt_id'].to_list()
        self.assertTrue(set(result).issubset(product_ids), "Result contains products not in the available products.")

    @patch('random.sample')
    def test_random_n_products_deterministic_seed(self, mock_random_sample):
        # Mocking random.sample to return a predictable set of products for deterministic testing
        mock_random_sample.return_value = ['product1', 'product2', 'product3', 'product4', 'product5']

        result = get_random_n_products_deterministic(
            self.haendler_bez,
            self.angebot_data,
            self.seal_date,
            n=self.n,
            seed=self.seed
        )

        # Ensure random.sample was called with the correct arguments
        args, kwargs = mock_random_sample.call_args
        product_ids = self.angebot_data['produkt_id'].to_list()
        self.assertTrue(set(args[0]).issubset(product_ids))  # Ensure valid sampling pool
        self.assertEqual(args[1], self.n)  # Ensure the correct number of products to sample

        # Assert the result matches the mocked return value
        self.assertEqual(result, mock_random_sample.return_value)


if __name__ == '__main__':
    unittest.main()
