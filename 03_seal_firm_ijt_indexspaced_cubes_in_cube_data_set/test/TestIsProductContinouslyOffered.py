import unittest
import datetime as dt
import polars as pl
from impl.static import is_product_continuously_offered


@DeprecationWarning
class TestIsProductContinuouslyOffered(unittest.TestCase):

    def test_product_continuously_offered(self):
        mock_produkt_id = 'product1'
        mock_haendler_bez = 'firm1'
        mock_seal_date_unix = "17.1.2022"

        angebot_data = pl.DataFrame({
            'produkt_id': ['product1'] * 10,
            'haendler_bez': ['firm1'] * 10,
            'dtimebegin': [
                dt.datetime(2021, 12, 20),
                dt.datetime(2021, 12, 27),
                dt.datetime(2022, 1, 3),
                dt.datetime(2022, 1, 10),
                dt.datetime(2022, 1, 17),
                dt.datetime(2022, 1, 24),
                dt.datetime(2022, 1, 31),
                dt.datetime(2022, 2, 7),
                dt.datetime(2022, 2, 14),
                dt.datetime(2022, 2, 21)
            ],
            'dtimeend': [
                dt.datetime(2021, 12, 26),
                dt.datetime(2022, 1, 2),
                dt.datetime(2022, 1, 9),
                dt.datetime(2022, 1, 16),
                dt.datetime(2022, 1, 23),
                dt.datetime(2022, 1, 30),
                dt.datetime(2022, 2, 6),
                dt.datetime(2022, 2, 13),
                dt.datetime(2022, 2, 20),
                dt.datetime(2022, 2, 27)
            ]
        })

        result = is_product_continuously_offered(
            mock_produkt_id,
            mock_haendler_bez,
            mock_seal_date_unix,
            angebot_data,
            weeks=4
        )

        self.assertTrue(result)

    def test_product_limit_continuously_offered(self):
        mock_produkt_id = 'product1'
        mock_haendler_bez = 'firm1'
        mock_seal_date_unix = "17.1.2022"

        angebot_data = pl.DataFrame({
            'produkt_id': ['product1'] * 9,
            'haendler_bez': ['firm1'] * 9,
            'dtimebegin': [
                dt.datetime(2021, 12, 20),
                dt.datetime(2022, 1, 3),  # Missing 2021, 12, 27 - 2022, 1, 2
                dt.datetime(2022, 1, 10),
                dt.datetime(2022, 1, 17),
                dt.datetime(2022, 1, 24),
                dt.datetime(2022, 1, 31),
                dt.datetime(2022, 2, 7),
                dt.datetime(2022, 2, 14),
                dt.datetime(2022, 2, 21)
            ],
            'dtimeend': [
                dt.datetime(2021, 12, 26),
                dt.datetime(2022, 1, 9),
                dt.datetime(2022, 1, 16),
                dt.datetime(2022, 1, 23),
                dt.datetime(2022, 1, 30),
                dt.datetime(2022, 2, 6),
                dt.datetime(2022, 2, 13),
                dt.datetime(2022, 2, 20),
                dt.datetime(2022, 2, 27)
            ]
        })

        result = is_product_continuously_offered(
            mock_produkt_id,
            mock_haendler_bez,
            mock_seal_date_unix,
            angebot_data,
            weeks=4
        )

        self.assertTrue(result)

    def test_product_not_continuously_offered(self):
        mock_produkt_id = 'product1'
        mock_haendler_bez = 'firm1'
        mock_seal_date_unix = "17.1.2022"

        angebot_data = pl.DataFrame({
            'produkt_id': ['product1'] * 8,
            'haendler_bez': ['firm1'] * 8,
            'dtimebegin': [
                dt.datetime(2021, 12, 20),
                dt.datetime(2022, 1, 10),  # Missing 2021, 12, 27 - 2022, 1, 9
                dt.datetime(2022, 1, 17),
                dt.datetime(2022, 1, 24),
                dt.datetime(2022, 1, 31),
                dt.datetime(2022, 2, 7),
                dt.datetime(2022, 2, 14),
                dt.datetime(2022, 2, 21)
            ],
            'dtimeend': [
                dt.datetime(2021, 12, 26),
                dt.datetime(2022, 1, 16),
                dt.datetime(2022, 1, 23),
                dt.datetime(2022, 1, 30),
                dt.datetime(2022, 2, 6),
                dt.datetime(2022, 2, 13),
                dt.datetime(2022, 2, 20),
                dt.datetime(2022, 2, 27)
            ]
        })

        result = is_product_continuously_offered(
            mock_produkt_id,
            mock_haendler_bez,
            mock_seal_date_unix,
            angebot_data,
            weeks=4
        )

        self.assertFalse(result)

    def test_product_continuously_offered_min_weeks(self):
        mock_produkt_id = 'product1'
        mock_haendler_bez = 'firm1'
        mock_seal_date_unix = "17.1.2022"

        angebot_data = pl.DataFrame({
            'produkt_id': ['product1'] * 4,
            'haendler_bez': ['firm1'] * 4,
            'dtimebegin': [
                dt.datetime(2022, 1, 10),
                dt.datetime(2022, 1, 17),
                dt.datetime(2022, 1, 24),
                dt.datetime(2022, 1, 31)
            ],
            'dtimeend': [
                dt.datetime(2022, 1, 16),
                dt.datetime(2022, 1, 23),
                dt.datetime(2022, 1, 30),
                dt.datetime(2022, 2, 6)
            ]
        })

        result = is_product_continuously_offered(
            mock_produkt_id,
            mock_haendler_bez,
            mock_seal_date_unix,
            angebot_data,
            weeks=3  # This should return False since weeks < 4
        )

        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
