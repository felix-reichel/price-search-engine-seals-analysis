import datetime as dt
import unittest
from unittest.mock import patch

from impl.db.duckdb_data_source import DuckDBDataSource
from impl.repository.repository import get_random_n_products_deterministic


class TestGetRandomNProductsDeterministic(unittest.TestCase):

    def setUp(self):
        # Setup DuckDB connection and create tables
        self.db = DuckDBDataSource(db_path=':memory:')
        self.db.conn.execute("""
            CREATE TABLE angebot (
                produkt_id STRING,
                haendler_bez STRING,
                dtimebegin TIMESTAMP,
                dtimeend TIMESTAMP
            )
        """)

        self.haendler_bez = 'firm1'
        self.seal_date = "17.1.2022"
        self.seed = 42
        self.n = 10  # Number of products to sample

        # Insert Angebot data for the firm into DuckDB
        angebot_data = [
            ('product1', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product2', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product3', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product4', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product5', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product6', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product7', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product8', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product9', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product10', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product11', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product12', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product13', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product14', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20)),
            ('product15', 'firm1', dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 20))
        ]
        self.db.conn.executemany("INSERT INTO angebot VALUES (?, ?, ?, ?)", angebot_data)

    def tearDown(self):
        # Close the DuckDB connection after each test
        self.db.close()

    def test_random_n_products_within_N(self):
        # Test where the number of products is greater than or equal to N
        result = get_random_n_products_deterministic(
            db=self.db,  # Pass DuckDB connection
            haendler_bez=self.haendler_bez,
            seal_date=self.seal_date,
            n=self.n,
            seed=self.seed
        )

        # Ensure the result contains exactly N products
        self.assertEqual(len(result), self.n)

        # Ensure that all selected products are from the inserted products list
        product_ids = [f'product{i}' for i in range(1, 16)]
        self.assertTrue(set(result).issubset(product_ids), "Result contains products not in the offered products.")

    def test_random_n_products_less_than_N(self):
        # Test where the number of products is less than N (e.g., only 5 products available)
        self.db.conn.execute(
            "DELETE FROM angebot WHERE produkt_id NOT IN ('product1', 'product2', 'product3', 'product4', 'product5')")

        result = get_random_n_products_deterministic(
            db=self.db,  # Pass DuckDB connection
            haendler_bez=self.haendler_bez,
            seal_date=self.seal_date,
            n=self.n,
            seed=self.seed
        )

        # Ensure the result contains all the available products (5 in this case)
        self.assertEqual(len(result), 5)

        # Ensure that all selected products are from the available products
        product_ids = ['product1', 'product2', 'product3', 'product4', 'product5']
        self.assertTrue(set(result).issubset(product_ids), "Result contains products not in the available products.")

    @patch('random.sample')
    def test_random_n_products_deterministic_seed(self, mock_random_sample):
        # Mocking random.sample to return a predictable set of products for deterministic testing
        mock_random_sample.return_value = ['product1', 'product2', 'product3', 'product4', 'product5']

        result = get_random_n_products_deterministic(
            db=self.db,  # Pass DuckDB connection
            haendler_bez=self.haendler_bez,
            seal_date=self.seal_date,
            n=self.n,
            seed=self.seed
        )

        # Ensure random.sample was called with the correct arguments
        args, kwargs = mock_random_sample.call_args
        product_ids = [f'product{i}' for i in range(1, 16)]
        self.assertTrue(set(args[0]).issubset(product_ids))  # Ensure valid sampling pool
        self.assertEqual(args[1], self.n)  # Ensure the correct number of products to sample

        # Assert the result matches the mocked return value
        self.assertEqual(result, mock_random_sample.return_value)


if __name__ == '__main__':
    unittest.main()
