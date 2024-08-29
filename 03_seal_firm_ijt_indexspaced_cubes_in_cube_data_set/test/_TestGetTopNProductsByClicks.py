import unittest
from unittest.mock import MagicMock
import datetime as dt
import polars as pl
from impl.static import get_top_n_products_by_clicks

"""
class TestGetTopNProductsByClicks(unittest.TestCase):
    def setUp(self):
        self.mock_haendler_bez_firm1 = MagicMock(return_value='firm1')
        self.mock_seal_date_str_firm1 = MagicMock(return_value='15.03.2022')
        self.mock_top_n_firm1 = MagicMock(return_value=2)

        self.mock_haendler_bez_firm2 = MagicMock(return_value='firm2')
        self.mock_seal_date_str_firm2 = MagicMock(return_value='15.09.2022')
        self.mock_top_n_firm2 = MagicMock(return_value=2)

        self.mock_haendler_bez_firm3 = MagicMock(return_value='firm3')
        self.mock_seal_date_str_firm3 = MagicMock(return_value='31.12.2022')
        self.mock_top_n_firm3 = MagicMock(return_value=2)

        self.mock_clicks_data = pl.DataFrame({
            'haendler_bez': [
                'firm1', 'firm1', 'firm1', 'firm1', 'firm1', 'firm1', 'firm1', 'firm1',  # firm1 clicks
                'firm2', 'firm2',  # firm2 clicks
                'firm3',  # firm3 click
                'firm1', 'firm1', 'firm1',  # Additional clicks for firm1 (outside observation period)
                'firm3'  # Additional click for firm3 (outside observation period)
            ],
            'timestamp': [
                int(dt.datetime(2021, 3, 14).timestamp()),  # prod1: 2 clicks before seal date
                int(dt.datetime(2021, 2, 10).timestamp()),
                int(dt.datetime(2022, 3, 10).timestamp()),  # prod1: 3 clicks within 8 weeks of seal date
                int(dt.datetime(2022, 2, 20).timestamp()),
                int(dt.datetime(2022, 3, 14).timestamp()),
                int(dt.datetime(2022, 9, 15).timestamp()),  # prod1: 4 clicks outside the 8-week period
                int(dt.datetime(2022, 2, 20).timestamp()),  # prod2: 2 clicks within 8 weeks of seal date
                int(dt.datetime(2022, 3, 10).timestamp()),

                int(dt.datetime(2022, 8, 1).timestamp()),  # prod3: 2 clicks within 8 weeks of seal date for firm2
                int(dt.datetime(2022, 9, 1).timestamp()),

                int(dt.datetime(2022, 12, 31).timestamp()),  # prod4: 1 click within 8 weeks of seal date for firm3

                int(dt.datetime(2022, 6, 10).timestamp()),  # Additional clicks (not in observation period)
                int(dt.datetime(2023, 1, 1).timestamp()),  # Additional clicks (not in observation period)
                int(dt.datetime(2023, 5, 10).timestamp()),

                int(dt.datetime(2023, 3, 15).timestamp())  # Additional click (not in observation period)
            ],
            'produkt_id': [
                'product1', 'product1', 'product1', 'product1', 'product1', 'product1', 'product2', 'product2',
                'product3', 'product3',
                'product4',
                'product1', 'product2', 'product1',
                'product4'
            ]
        })

    def test_get_top_products_by_clicks_firm1(self):
        actual_top_products = get_top_n_products_by_clicks(
            self.mock_haendler_bez_firm1.return_value,
            self.mock_seal_date_str_firm1.return_value,
            self.mock_clicks_data,
            2
        )
        expected_top_products = ['product1', 'product2']
        self.assertEqual(actual_top_products, expected_top_products)

    def test_get_top_products_by_clicks_firm1_Top1(self):
        actual_top_products = get_top_n_products_by_clicks(
            self.mock_haendler_bez_firm1.return_value,
            self.mock_seal_date_str_firm1.return_value,
            self.mock_clicks_data,
            1
        )
        expected_top_products = ['product1']
        self.assertEqual(actual_top_products, expected_top_products)

    def test_get_top_products_by_clicks_firm1_Top1_NewSeal(self):
        actual_top_products = get_top_n_products_by_clicks(
            self.mock_haendler_bez_firm1.return_value,
            "15.06.2024",
            self.mock_clicks_data,
            1
        )
        expected_top_products = []
        self.assertEqual(actual_top_products, expected_top_products)

    def test_get_top_products_by_clicks_firm2(self):
        actual_top_products = get_top_n_products_by_clicks(
            self.mock_haendler_bez_firm2.return_value,
            self.mock_seal_date_str_firm2.return_value,
            self.mock_clicks_data,
            2
        )
        expected_top_products = ['product3']
        self.assertEqual(actual_top_products, expected_top_products)

    def test_get_top_products_by_clicks_firm3(self):
        actual_top_products = get_top_n_products_by_clicks(
            self.mock_haendler_bez_firm3.return_value,
            self.mock_seal_date_str_firm3.return_value,
            self.mock_clicks_data,
            2
        )
        expected_top_products = ['product4']
        self.assertEqual(actual_top_products, expected_top_products)


if __name__ == '__main__':
    unittest.main()
    
"""
