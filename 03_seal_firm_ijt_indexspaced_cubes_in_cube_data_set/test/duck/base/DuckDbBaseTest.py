from unittest import TestCase
from unittest.mock import patch

from impl.db.datasource import DuckDBDataSource


class DuckDbBaseTest(TestCase):
    def setUp(self):
        # Patch thread config
        patcher = patch('impl.db.datasource.ApplicationThreadConfig.calculate_thread_distribution', return_value={
            "duckdb_thread_count": 8,
            "duckdb_background_thread_count": 2,
        })
        self.mock_thread_config = patcher.start()
        self.addCleanup(patcher.stop)

        # Use a new instance and bypass singleton to ensure a fresh connection for each test
        self.db = DuckDBDataSource(db_path=':memory:', bypass_singleton=True)

    def tearDown(self):
        # Close the connection after each test
        self.db.close()
