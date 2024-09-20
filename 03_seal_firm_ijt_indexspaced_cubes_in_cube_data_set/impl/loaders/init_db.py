import logging
from CONFIG import PARQUET_FILES_DIR, SEAL_CHANGE_FIRMS, FILTERED_HAENDLER_BEZ
from SCHEMA_CONFIG import INITIAL_TABLE_SCHEMAS
from impl.db.datasource import DuckDBDataSource

logger = logging.getLogger(__name__)


class DatabaseInitializer:
    def __init__(self, db: DuckDBDataSource):
        self.db = db

    def create_tables(self):
        self.db.initialize_file_log_table()
        for table_name, schema in INITIAL_TABLE_SCHEMAS.items():
            try:
                self.db.query(schema)
                logger.info(f"Table '{table_name}' created successfully.")
            except Exception as e:
                logger.error(f"Error creating table '{table_name}': {e}")

    def drop_tables(self, tables=None):
        default_tables = ['seal_change_firms', 'filtered_haendler_bez', 'products', 'retailers', 'file_log']
        tables = tables or default_tables

        for table in tables:
            try:
                self.db.query(f"DROP TABLE IF EXISTS {table}")
                logger.info(f"Table '{table}' dropped successfully.")
            except Exception as e:
                logger.error(f"Error dropping table '{table}': {e}")

    def load_data_into_tables(self):
        try:
            self.db.load_csv_to_table(SEAL_CHANGE_FIRMS, 'seal_change_firms')
            self.db.load_csv_to_table(FILTERED_HAENDLER_BEZ, 'filtered_haendler_bez')
            self.db.load_parquet_to_table(PARQUET_FILES_DIR / 'produkt.parquet', 'products')
            self.db.load_parquet_to_table(PARQUET_FILES_DIR / 'haendler.parquet', 'retailers')

        except Exception as e:
            logger.error(f"Error loading data into tables: {e}")

    def initialize_database(self):
        self.drop_tables()
        self.create_tables()
        self.load_data_into_tables()
