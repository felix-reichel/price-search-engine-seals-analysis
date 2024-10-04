import datetime as dt
import unittest
from unittest.mock import patch

from ..base.DuckDbBaseTest import DuckDbBaseTest
from impl.repository.offers_repository import OffersRepository
from impl.service.offers_service import OffersService


class TestGetRandomNProductsDeterministic(DuckDbBaseTest):

    def setUp(self):
        super().setUp()
        self.db.conn.execute("""
            CREATE TABLE angebot (
                produkt_id STRING,
                haendler_bez STRING,
                dtimebegin BIGINT,
                dtimeend BIGINT
            )
        """)

        self.haendler_bez = 'firm1'
        self.seal_date_str = "17.1.2022"
        self.seed = 42
        self.n = 5  # Number of products to sample
        self.time_window = 4  # Observation window in weeks

        # Insert Angebot data for the firm into DuckDB
        angebot_data = [
            # Within the observation window
            ('product1', 'firm1', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product2', 'firm1', int(dt.datetime(2022, 1, 12).timestamp()), int(dt.datetime(2022, 1, 22).timestamp())),
            ('product3', 'firm1', int(dt.datetime(2022, 1, 14).timestamp()), int(dt.datetime(2022, 1, 24).timestamp())),
            ('product4', 'firm1', int(dt.datetime(2022, 1, 16).timestamp()), int(dt.datetime(2022, 1, 26).timestamp())),
            ('product5', 'firm1', int(dt.datetime(2022, 1, 18).timestamp()), int(dt.datetime(2022, 1, 28).timestamp())),
            ('product6', 'firm1', int(dt.datetime(2022, 1, 20).timestamp()), int(dt.datetime(2022, 1, 30).timestamp())),
            # Outside the observation window
            ('product7', 'firm1', int(dt.datetime(2022, 3, 7).timestamp()), int(dt.datetime(2022, 3, 10).timestamp())),
            ('product8', 'firm1', int(dt.datetime(2021, 12, 1).timestamp()), int(dt.datetime(2021, 12, 10).timestamp())),
        ]
        self.db.conn.executemany("INSERT INTO angebot VALUES (?, ?, ?, ?)", angebot_data)

        # Instantiate repository and service
        self.repository = OffersRepository(self.db)
        self.service = OffersService(self.repository)

    def test_random_n_products_within_N(self):
        # Test where the number of products within the observation window is greater than or equal to N
        result = self.service.get_random_n_products_deterministic(
            haendler_bez=self.haendler_bez,
            seal_date_str=self.seal_date_str,
            n=self.n,
            seed=self.seed,
            time_window_around_seal=self.time_window  # Apply time window
        )

        # Ensure the result contains exactly N products
        self.assertEqual(len(result), self.n)  # There are 5 valid products in the window

        # Ensure that all selected products are from the valid products list
        valid_product_ids = ['product1', 'product2', 'product3', 'product4', 'product5', 'product6']
        self.assertTrue(set(result).issubset(valid_product_ids), "Result contains products not in the observation window.")

    def test_random_n_products_less_than_N(self):
        # Test where the number of products is less than N (e.g., only 1 product in the observation window)
        self.db.conn.execute(
            "DELETE FROM angebot WHERE produkt_id IN ('product1', 'product2', 'product3', 'product4', 'product5')"
        )

        result = self.service.get_random_n_products_deterministic(
            haendler_bez=self.haendler_bez,
            seal_date_str=self.seal_date_str,
            n=self.n,
            seed=self.seed,
            time_window_around_seal=self.time_window
        )

        # Ensure the result contains all the available products (1 in this case)
        self.assertEqual(len(result), 1)

        # Ensure that all selected products are from the available products
        product_ids = ['product1', 'product2', 'product3', 'product4', 'product5', 'product6']
        self.assertTrue(set(result).issubset(product_ids), "Result contains products not in the available products.")

    @patch('random.sample')
    def test_random_n_products_deterministic_seed(self, mock_random_sample):
        # Mocking random.sample to return a predictable set of products for deterministic testing
        mock_random_sample.return_value = ['product1', 'product2', 'product3', 'product4', 'product5']

        result = self.service.get_random_n_products_deterministic(
            haendler_bez=self.haendler_bez,
            seal_date_str=self.seal_date_str,
            n=self.n,
            seed=self.seed,
            time_window_around_seal=self.time_window
        )

        # Ensure random.sample was called with the correct arguments
        args, kwargs = mock_random_sample.call_args
        valid_product_ids = ['product1', 'product2', 'product3', 'product4', 'product5', 'product6']
        self.assertTrue(set(args[0]).issubset(valid_product_ids))  # Ensure valid sampling pool
        self.assertEqual(args[1], self.n)  # Ensure the correct number of products to sample

        # Assert the result matches the mocked return value
        self.assertEqual(result, mock_random_sample.return_value)


if __name__ == '__main__':
    unittest.main()
