import unittest
import datetime as dt
from impl.db.duckdb_data_source import DuckDBDataSource
from impl.repository.repository import ProductRepository
from impl.service.service import ProductService
from impl.static import calculate_running_var_t_from_u


class TestGetOfferedWeeksDuckDb(unittest.TestCase):

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

        # Insert sample data into the 'angebot' table
        angebot_data = [
            ('P1', 'F1', int(dt.datetime(2022, 12, 25).timestamp()), int(dt.datetime(2023, 1, 2).timestamp())),
            ('P1', 'F1', int(dt.datetime(2023, 8, 15).timestamp()), int(dt.datetime(2023, 8, 18).timestamp())),
            ('P2', 'F2', int(dt.datetime(2021, 8, 14).timestamp()), int(dt.datetime(2021, 8, 15).timestamp())),
            ('P1', 'F1', int(dt.datetime(2024, 1, 1).timestamp()), int(dt.datetime(2024, 1, 7).timestamp()))
        ]

        self.db.conn.executemany("INSERT INTO angebot VALUES (?, ?, ?, ?)", angebot_data)

        # Instantiate repository and service
        self.repository = ProductRepository(self.db)
        self.service = ProductService(self.repository)

    def tearDown(self):
        # Close the DuckDB connection after each test
        self.db.close()

    def test_get_offered_weeks_within_observation_window(self):
        prod_id = 'P1'
        firm_id = 'F1'
        seal_date = "15.8.2023"  # Seal date

        # Calculate the expected weeks
        expected_weeks = {
            int(calculate_running_var_t_from_u(int(dt.datetime(2023, 8, 17).timestamp()))),
            int(calculate_running_var_t_from_u(int(dt.datetime(2024, 1, 7).timestamp()))),
        }

        # Call the function from ProductService with DuckDB as the data source
        result_weeks = self.service.get_offered_weeks(prod_id, firm_id, seal_date)

        # Assert the result matches the expected weeks
        self.assertEqual(expected_weeks, result_weeks)


if __name__ == '__main__':
    unittest.main()