import logging

import duckdb

import CONFIG

logger = logging.getLogger(__name__)


class DuckDBDataSource:
    def __init__(self,
                 db_path='localduck.db',  # ':memory:'
                 threads=CONFIG.THREADS):
        self.conn = duckdb.connect(db_path, config={'threads': threads})
        # self.set_threads(threads)

    """
    def set_threads(self,
                    threads):

        threads = max(threads, 32)
        logger.info(f"Setting DuckDB to use {threads} threads.")
        # set via PRAGMA threads
        self.conn.execute(f"PRAGMA threads={threads}")
    """

    def load_csv_to_table(self,
                          csv_path,
                          table_name,
                          separator=CONFIG.CSV_DELIM_STYLE):

        logger.info(f"Loading CSV from {csv_path} into table {table_name}")

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT * FROM read_csv_auto('{csv_path}', delim='{separator}')
        """)

    def load_parquet_to_table(
            self,
            parquet_path,
            table_name,
            columns=None):

        logger.info(f"Loading Parquet from {parquet_path} into table {table_name}")
        if columns:
            column_str = ", ".join(columns)
        else:
            column_str = "*"

        self.conn.execute(f"""
            CREATE OR REPLACE TABLE {table_name} AS
            SELECT {column_str} FROM read_parquet('{parquet_path}')
        """)

    def append_parquet_to_table(
            self,
            parquet_path,
            table_name,
            columns=None):
        logger.info(f"Appending Parquet from {parquet_path} into table {table_name}")
        if columns:
            column_str = ", ".join(columns)
        else:
            column_str = "*"

        self.conn.execute(f"""
            INSERT INTO {table_name} 
            SELECT {column_str} FROM read_parquet('{parquet_path}')
        """)

    def query(self,
              query_str):

        logger.info(f"Executing query: {query_str}")

        return self.conn.execute(query_str).df()

    def close(self):
        self.conn.close()
