import datetime as dt
import unittest

from ..base.DuckDbBaseTest import DuckDbBaseTest
from impl.repository.offers_repository import OffersRepository
from impl.service.offers_service import OffersService


class TestFilterContinuouslyOfferedProductsDuckDb(DuckDbBaseTest):

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
        self.seal_date_unix = "17.1.2022"
        self.top_products = ['product1', 'product2', 'product3']

        # Insert mock angebot_data into DuckDB
        angebot_data = [
            # product1: continuous
            ('product1', 'firm1', int(dt.datetime(2021, 12, 20).timestamp()), int(dt.datetime(2021, 12, 26).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2021, 12, 27).timestamp()), int(dt.datetime(2022, 1, 2).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 3).timestamp()), int(dt.datetime(2022, 1, 9).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 16).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 17).timestamp()), int(dt.datetime(2022, 1, 23).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 24).timestamp()), int(dt.datetime(2022, 1, 30).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 31).timestamp()), int(dt.datetime(2022, 2, 6).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 2, 7).timestamp()), int(dt.datetime(2022, 2, 13).timestamp())),

            # product2: missing one week
            ('product2', 'firm1', int(dt.datetime(2021, 12, 20).timestamp()), int(dt.datetime(2021, 12, 26).timestamp())),
            ('product2', 'firm1', int(dt.datetime(2022, 1, 3).timestamp()), int(dt.datetime(2022, 1, 9).timestamp())),
            ('product2', 'firm1', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 16).timestamp())),
            ('product2', 'firm1', int(dt.datetime(2022, 1, 17).timestamp()), int(dt.datetime(2022, 1, 23).timestamp())),
            ('product2', 'firm1', int(dt.datetime(2022, 1, 24).timestamp()), int(dt.datetime(2022, 1, 30).timestamp())),
            ('product2', 'firm1', int(dt.datetime(2022, 1, 31).timestamp()), int(dt.datetime(2022, 2, 6).timestamp())),
            ('product2', 'firm1', int(dt.datetime(2022, 2, 7).timestamp()), int(dt.datetime(2022, 2, 13).timestamp())),
            ('product2', 'firm1', int(dt.datetime(2022, 2, 14).timestamp()), int(dt.datetime(2022, 2, 20).timestamp())),

            # product3: missing two weeks
            ('product3', 'firm1', int(dt.datetime(2021, 12, 20).timestamp()), int(dt.datetime(2021, 12, 26).timestamp())),
            ('product3', 'firm1', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 16).timestamp())),
            ('product3', 'firm1', int(dt.datetime(2022, 1, 17).timestamp()), int(dt.datetime(2022, 1, 23).timestamp())),
            ('product3', 'firm1', int(dt.datetime(2022, 1, 24).timestamp()), int(dt.datetime(2022, 1, 30).timestamp())),
            ('product3', 'firm1', int(dt.datetime(2022, 1, 31).timestamp()), int(dt.datetime(2022, 2, 6).timestamp())),
            ('product3', 'firm1', int(dt.datetime(2022, 2, 7).timestamp()), int(dt.datetime(2022, 2, 13).timestamp())),
            ('product3', 'firm1', int(dt.datetime(2022, 2, 14).timestamp()), int(dt.datetime(2022, 2, 20).timestamp()))
        ]

        self.db.conn.executemany("INSERT INTO angebot VALUES (?, ?, ?, ?)", angebot_data)

        # Instantiate the repository and service
        self.repository = OffersRepository(self.db)
        self.service = OffersService(self.repository)

    def test_filter_continuously_offered_products(self):
        # Expected to pass for product1 and product2, fail for product3
        HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT = 4

        result = self.service.filter_continuously_offered_products(
            haendler_bez=self.haendler_bez,
            top_products=self.top_products,
            seal_date_str=self.seal_date_unix,
            week_amount=HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT
        )

        self.assertEqual(['product1', 'product2'], result)


if __name__ == '__main__':
    unittest.main()
