import unittest
import datetime as dt
from impl.db.datasource import DuckDBDataSource
from impl.repository.OffersRepository import OffersRepository
from impl.service.OffersService import OffersService


class TestIsProductContinuouslyOfferedDuckDb(unittest.TestCase):

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

        # Instantiate repository and service
        self.repository = OffersRepository(self.db)
        self.service = OffersService(self.repository)

    def tearDown(self):
        # Close the DuckDB connection after each test
        self.db.close()

    def test_product_continuously_offered(self):
        # Insert mock angebot_data into DuckDB
        angebot_data = [
            ('product1', 'firm1', int(dt.datetime(2021, 12, 20).timestamp()), int(dt.datetime(2021, 12, 26).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2021, 12, 27).timestamp()), int(dt.datetime(2022, 1, 2).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 3).timestamp()), int(dt.datetime(2022, 1, 9).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 16).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 17).timestamp()), int(dt.datetime(2022, 1, 23).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 24).timestamp()), int(dt.datetime(2022, 1, 30).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 31).timestamp()), int(dt.datetime(2022, 2, 6).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 2, 7).timestamp()), int(dt.datetime(2022, 2, 13).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 2, 14).timestamp()), int(dt.datetime(2022, 2, 20).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 2, 21).timestamp()), int(dt.datetime(2022, 2, 27).timestamp()))
        ]
        self.db.conn.executemany("INSERT INTO angebot VALUES (?, ?, ?, ?)", angebot_data)

        # Call the service method to check if the product is continuously offered
        result = self.service.is_product_continuously_offered(
            produkt_id='product1',
            haendler_bez='firm1',
            seal_date_str="17.1.2022",
            weeks=4
        )

        self.assertTrue(result)

    def test_product_limit_continuously_offered(self):
        # Insert mock angebot_data into DuckDB
        angebot_data = [
            ('product1', 'firm1', int(dt.datetime(2021, 12, 20).timestamp()), int(dt.datetime(2021, 12, 26).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 3).timestamp()), int(dt.datetime(2022, 1, 9).timestamp())),  # Missing week 27th Dec to 2nd Jan
            ('product1', 'firm1', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 16).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 17).timestamp()), int(dt.datetime(2022, 1, 23).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 24).timestamp()), int(dt.datetime(2022, 1, 30).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 31).timestamp()), int(dt.datetime(2022, 2, 6).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 2, 7).timestamp()), int(dt.datetime(2022, 2, 13).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 2, 14).timestamp()), int(dt.datetime(2022, 2, 20).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 2, 21).timestamp()), int(dt.datetime(2022, 2, 27).timestamp()))
        ]
        self.db.conn.executemany("INSERT INTO angebot VALUES (?, ?, ?, ?)", angebot_data)

        result = self.service.is_product_continuously_offered(
            produkt_id='product1',
            haendler_bez='firm1',
            seal_date_str="17.1.2022",
            weeks=4
        )

        self.assertTrue(result)

    def test_product_not_continuously_offered(self):
        # Insert mock angebot_data into DuckDB
        angebot_data = [
            ('product1', 'firm1', int(dt.datetime(2021, 12, 20).timestamp()), int(dt.datetime(2021, 12, 26).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 16).timestamp())),  # Missing two weeks
            ('product1', 'firm1', int(dt.datetime(2022, 1, 17).timestamp()), int(dt.datetime(2022, 1, 23).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 24).timestamp()), int(dt.datetime(2022, 1, 30).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 31).timestamp()), int(dt.datetime(2022, 2, 6).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 2, 7).timestamp()), int(dt.datetime(2022, 2, 13).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 2, 14).timestamp()), int(dt.datetime(2022, 2, 20).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 2, 21).timestamp()), int(dt.datetime(2022, 2, 27).timestamp()))
        ]
        self.db.conn.executemany("INSERT INTO angebot VALUES (?, ?, ?, ?)", angebot_data)

        result = self.service.is_product_continuously_offered(
            produkt_id='product1',
            haendler_bez='firm1',
            seal_date_str="17.1.2022",
            weeks=4
        )

        self.assertFalse(result)

    def test_product_continuously_offered_min_weeks(self):
        # Insert mock angebot_data into DuckDB
        angebot_data = [
            ('product1', 'firm1', int(dt.datetime(2022, 1, 10).timestamp()), int(dt.datetime(2022, 1, 16).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 17).timestamp()), int(dt.datetime(2022, 1, 23).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 24).timestamp()), int(dt.datetime(2022, 1, 30).timestamp())),
            ('product1', 'firm1', int(dt.datetime(2022, 1, 31).timestamp()), int(dt.datetime(2022, 2, 6).timestamp()))
        ]
        self.db.conn.executemany("INSERT INTO angebot VALUES (?, ?, ?, ?)", angebot_data)

        result = self.service.is_product_continuously_offered(
            produkt_id='product1',
            haendler_bez='firm1',
            seal_date_str="17.1.2022",
            weeks=3  # This should return False since weeks < 4
        )

        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
