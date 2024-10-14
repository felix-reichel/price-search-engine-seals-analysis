import datetime as dt
import unittest

from impl.repository.clicks_repository import ClicksRepository
from impl.service.clicks_service import ClicksService
from ..base.DuckDbBaseTest import DuckDbBaseTest


class TestGetTopNProductsByClicksDuckDb(DuckDbBaseTest):

    def setUp(self):
        super().setUp()
        self.db.conn.execute("""
            CREATE TABLE clicks (
                haendler_bez STRING,
                timestamp BIGINT,
                produkt_id STRING
            )
        """)

        # Insert sample data into the 'clicks' table
        clicks_data = [
            # firm1 clicks
            ('firm1', int(dt.datetime(2021, 3, 14).timestamp()), 'product1'),  # prod1: 2 clicks before seal date
            ('firm1', int(dt.datetime(2021, 2, 10).timestamp()), 'product1'),
            ('firm1', int(dt.datetime(2022, 3, 10).timestamp()), 'product1'),  # prod1: 3 clicks within 8 weeks of seal date
            ('firm1', int(dt.datetime(2022, 2, 20).timestamp()), 'product1'),
            ('firm1', int(dt.datetime(2022, 3, 14).timestamp()), 'product1'),
            ('firm1', int(dt.datetime(2022, 9, 15).timestamp()), 'product1'),  # prod1: 4 clicks outside the 8-week period
            ('firm1', int(dt.datetime(2022, 2, 20).timestamp()), 'product2'),  # prod2: 2 clicks within 8 weeks of seal date
            ('firm1', int(dt.datetime(2022, 3, 10).timestamp()), 'product2'),

            # firm2 clicks
            ('firm2', int(dt.datetime(2022, 8, 1).timestamp()), 'product3'),  # prod3: 2 clicks within 8 weeks of seal date for firm2
            ('firm2', int(dt.datetime(2022, 9, 1).timestamp()), 'product3'),

            # firm3 click
            ('firm3', int(dt.datetime(2022, 12, 31).timestamp()), 'product4'),  # prod4: 1 click within 8 weeks of seal date for firm3

            # Additional clicks (outside observation period)
            ('firm1', int(dt.datetime(2022, 6, 10).timestamp()), 'product1'),
            ('firm1', int(dt.datetime(2023, 1, 1).timestamp()), 'product2'),
            ('firm1', int(dt.datetime(2023, 5, 10).timestamp()), 'product1'),
            ('firm3', int(dt.datetime(2023, 3, 15).timestamp()), 'product4')
        ]

        self.db.conn.executemany("INSERT INTO clicks VALUES (?, ?, ?)", clicks_data)

        # Instantiate repository and service
        self.repository = ClicksRepository(self.db)
        self.service = ClicksService(self.repository)

    def test_get_top_products_by_clicks_firm1(self):
        actual_top_products = self.service.get_top_n_products_by_clicks(
            haendler_bez='firm1',
            seal_date_str='15.03.2022',
            top_n=2
        )
        expected_top_products = ['product1', 'product2']
        self.assertListEqual(expected_top_products, actual_top_products)

    def test_get_top_products_by_clicks_firm1_Top1(self):
        actual_top_products = self.service.get_top_n_products_by_clicks(
            haendler_bez='firm1',
            seal_date_str='15.03.2022',
            top_n=1
        )
        expected_top_products = ['product1']
        self.assertListEqual(expected_top_products, actual_top_products)

    def test_get_top_products_by_clicks_firm1_Top1_NewSeal(self):
        actual_top_products = self.service.get_top_n_products_by_clicks(
            haendler_bez='firm1',
            seal_date_str='15.06.2024',
            top_n=1
        )
        expected_top_products = []
        self.assertListEqual(expected_top_products, actual_top_products)

    def test_get_top_products_by_clicks_firm2(self):
        actual_top_products = self.service.get_top_n_products_by_clicks(
            haendler_bez='firm2',
            seal_date_str='15.09.2022',
            top_n=2
        )
        expected_top_products = ['product3']
        self.assertListEqual(expected_top_products, actual_top_products)

    def test_get_top_products_by_clicks_firm3(self):
        actual_top_products = self.service.get_top_n_products_by_clicks(
            haendler_bez='firm3',
            seal_date_str='31.12.2022',
            top_n=2
        )
        expected_top_products = ['product4']
        self.assertListEqual(expected_top_products, actual_top_products)


if __name__ == '__main__':
    unittest.main()
