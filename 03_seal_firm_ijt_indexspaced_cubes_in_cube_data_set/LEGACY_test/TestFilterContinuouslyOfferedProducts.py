import unittest
import datetime as dt
import polars as pl

from impl.static import filter_continuously_offered_products

class TestFilterContinuouslyOfferedProducts(unittest.TestCase):

    def setUp(self):

        # Common setup for all tests
        self.haendler_bez = 'firm1'
        self.seal_date_unix = int(dt.datetime(2022, 1, 17).timestamp())
        self.top_products = ['product1', 'product2', 'product3']

        # Create a mock angebot_data dataframe
        self.angebot_data = pl.DataFrame({
            'produkt_id': ['product1'] * 8 + ['product2'] * 8 + ['product3'] * 7,
            'haendler_bez': ['firm1'] * 23,
            'dtimebegin': [
                # product1: continuous
                dt.datetime(2021, 12, 20), dt.datetime(2021, 12, 27), dt.datetime(2022, 1, 3),
                dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 17), dt.datetime(2022, 1, 24),
                dt.datetime(2022, 1, 31), dt.datetime(2022, 2, 7),

                # product2: missing one week
                dt.datetime(2021, 12, 20), dt.datetime(2022, 1, 3),
                dt.datetime(2022, 1, 10), dt.datetime(2022, 1, 17),
                dt.datetime(2022, 1, 24), dt.datetime(2022, 1, 31),
                dt.datetime(2022, 2, 7), dt.datetime(2022, 2, 14),

                # product3: missing two weeks
                dt.datetime(2021, 12, 20), dt.datetime(2022, 1, 10),
                dt.datetime(2022, 1, 17), dt.datetime(2022, 1, 24),
                dt.datetime(2022, 1, 31), dt.datetime(2022, 2, 7),
                dt.datetime(2022, 2, 14)
            ],
            'dtimeend': [
                # product1: continuous
                dt.datetime(2021, 12, 26), dt.datetime(2022, 1, 2), dt.datetime(2022, 1, 9),
                dt.datetime(2022, 1, 16), dt.datetime(2022, 1, 23), dt.datetime(2022, 1, 30),
                dt.datetime(2022, 2, 6), dt.datetime(2022, 2, 13),

                # product2: missing one week
                dt.datetime(2021, 12, 26), dt.datetime(2022, 1, 9),
                dt.datetime(2022, 1, 16), dt.datetime(2022, 1, 23),
                dt.datetime(2022, 1, 30), dt.datetime(2022, 2, 6),
                dt.datetime(2022, 2, 13), dt.datetime(2022, 2, 20),

                # product3: missing two weeks
                dt.datetime(2021, 12, 26), dt.datetime(2022, 1, 16),
                dt.datetime(2022, 1, 23), dt.datetime(2022, 1, 30),
                dt.datetime(2022, 2, 6), dt.datetime(2022, 2, 13),
                dt.datetime(2022, 2, 20)
            ]
        })

    def test_filter_continuously_offered_products(self):
        # Expected to pass for product1 and product2, fail for product3
        HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT = 4

        result = filter_continuously_offered_products(
            self.haendler_bez,
            self.top_products,
            self.seal_date_unix,
            self.angebot_data,
            HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT
        )

        self.assertEqual(result, ['product1', 'product2'])


if __name__ == '__main__':
    unittest.main()
