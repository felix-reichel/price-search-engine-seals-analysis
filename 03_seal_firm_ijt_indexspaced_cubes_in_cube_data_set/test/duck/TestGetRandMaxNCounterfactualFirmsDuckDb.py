import unittest
import datetime as dt
from unittest.mock import patch
from impl.db.datasource import DuckDBDataSource
from impl.repository.OffersRepository import OffersRepository
from impl.service.OffersService import OffersService


class TestGetRandMaxNCounterfactualFirmsDuckDb(unittest.TestCase):

    def setUp(self):
        # Setup DuckDB connection and create the 'angebot' table
        self.db = DuckDBDataSource(db_path=':memory:')
        self.db.conn.execute("""
            CREATE TABLE angebot (
                produkt_id STRING,
                haendler_bez STRING,
                dtimebegin BIGINT,
                dtimeend BIGINT
            )
        """)

        # Insert mock data into DuckDB
        angebot_data = [
            ('product1', 'firm1', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm2', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm3', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm4', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm5', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm6', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm7', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm8', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm9', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm10', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm11', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm12', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm13', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm14', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm15', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm16', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm17', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm18', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm19', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm20', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm21', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm22', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm23', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm24', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm25', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm26', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm27', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            ('product1', 'firm28', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp()))
        ]
        self.db.conn.executemany("INSERT INTO angebot VALUES (?, ?, ?, ?)", angebot_data)

        # Define product, seal firms, and allowed firms
        self.product_id = 'product1'
        self.seal_date = "17.1.2022"
        self.seal_firms = ['firm1', 'firm2']
        self.allowed_firms = [
            'firm3', 'firm4', 'firm5', 'firm6', 'firm7', 'firm8', 'firm9', 'firm10',
            'firm11', 'firm12', 'firm13', 'firm14', 'firm15', 'firm16', 'firm17', 'firm18',
            'firm19', 'firm20', 'firm21', 'firm22', 'firm23', 'firm24', 'firm25', 'firm26',
            'firm27', 'firm28'
        ]

        # Instantiate repository and service
        self.repository = OffersRepository(self.db)
        self.service = OffersService(self.repository)

        # Expected firms
        self.expected_firms = set(self.allowed_firms)

    def tearDown(self):
        # Close the DuckDB connection after each test
        self.db.close()

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
        result = self.service.get_rand_max_N_counterfactual_firms(
            product_id=self.product_id,
            seal_date_str=self.seal_date,
            seal_firms=self.seal_firms,
            allowed_firms=self.allowed_firms
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

        result = self.service.get_rand_max_N_counterfactual_firms(
            product_id=self.product_id,
            seal_date_str=self.seal_date,
            seal_firms=self.seal_firms,
            allowed_firms=self.allowed_firms
        )

        # Ensure random.sample was called with the correct elements
        mock_random_sample.assert_called()  # Ensure that random.sample was actually called

        # Extract the args passed to random.sample
        args, kwargs = mock_random_sample.call_args

        # Ensure it sampled 10 elements
        self.assertEqual(args[1], 10)  # Ensure it sampled 10 elements

        # Ensure that firms are valid and within the allowed subset
        self.assert_firms_are_valid(result)
        # Ensure the result contains exactly 10 firms
        self.assertEqual(len(result), 10)


if __name__ == '__main__':
    unittest.main()
