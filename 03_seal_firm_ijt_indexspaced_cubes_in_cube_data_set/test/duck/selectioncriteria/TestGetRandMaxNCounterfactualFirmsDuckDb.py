import datetime as dt
import unittest
import polars as pl
from unittest.mock import patch

from impl.repository.offers_repository import OffersRepository
from impl.service.offers_service import OffersService
from ..base.DuckDbBaseTest import DuckDbBaseTest


class TestGetRandMaxNCounterfactualFirmsDuckDb(DuckDbBaseTest):

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
            (
            'product1', 'firm10', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm11', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm12', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm13', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm14', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm15', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm16', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm17', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm18', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm19', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm20', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm21', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm22', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm23', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm24', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm25', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm26', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
            (
            'product1', 'firm27', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 20).timestamp())),
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

    def assert_firms_are_valid(self, result):
        """
        Helper method to assert that the result contains valid firms.
        Ensures that:
        - The result firms are in the expected firms.
        - The result firms do not include any seal firms.
        - The result firms do not include any disallowed firms.
        """
        result_set = set(result)
        self.assertTrue(result_set.issubset(self.expected_firms), "Result contains firms not in expected firms.")
        self.assertTrue(all(firm not in self.seal_firms for firm in result), "Result contains seal firms.")
        self.assertTrue(all(firm not in self.allowed_firms for firm in result), "Result contains disallowed firms.")

    def test_counterfactual_firms_within_N(self):
        # Test where the number of counterfactual firms is less than or equal to N (10 in this case)
        result = self.service.get_rand_max_N_counterfactual_firms(
            product_id=self.product_id,
            seal_date_str=self.seal_date,
            seal_firms=pl.Series(self.seal_firms),
            allowed_firms=pl.Series(self.allowed_firms)
        )

        # Assert that firms are valid and within the allowed subset
        self.assert_firms_are_valid(result)
        # Ensure the result contains at most 10 firms
        self.assertLessEqual(len(result), 10)


if __name__ == '__main__':
    unittest.main()
