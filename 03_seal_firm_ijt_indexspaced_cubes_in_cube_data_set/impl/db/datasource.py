import logging
import pathlib
from multiprocessing import cpu_count
from datetime import datetime
from typing import List, Optional
import duckdb
import polars as pl
import CONFIG

logger = logging.getLogger(__name__)


class DuckDBDataSource:
    def __init__(self, db_path: str = CONFIG.DUCKDB_PATH, threads: int = CONFIG.DUCKDB_THREADS):
        self.conn = duckdb.connect(db_path, config={'threads': threads})
        self.set_threads(threads)
        self.initialize_file_log_table()

    def set_threads(self, threads: int):
        effective_threads = max(threads, cpu_count() - 1)
        logger.info(f"Setting DuckDB to use {effective_threads} threads.")
        self.conn.execute(f"PRAGMA threads={effective_threads}")

    def initialize_file_log_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS file_log (
                file_name TEXT PRIMARY KEY,
                insert_timestamp TIMESTAMP
            )
        """)
        logger.info("Initialized file_log table.")

    def _has_file_been_inserted(self, file_name: str) -> bool:
        result = self.conn.execute(
            "SELECT COUNT(*) AS count FROM file_log WHERE file_name = ?",
            (file_name,)
        ).fetchone()
        return result[0] > 0

    def _log_file_insertion(self, file_name: str):
        insert_time = datetime.now()
        self.conn.execute(
            "INSERT INTO file_log (file_name, insert_timestamp) VALUES (?, ?)",
            (file_name, insert_time)
        )
        logger.info(f"Logged file insertion: {file_name} at {insert_time}")

    def _skip_load(self, file_path: str) -> bool:
        if self._has_file_been_inserted(file_path):
            logger.info(f"File {file_path} has already been inserted. Skipping load.")
            return True
        return False

    def _load_data(self, file_path: str, query_template: str, params: tuple):
        """Helper function to load data using the given query template and parameters."""
        logger.info(f"Loading data from {file_path}")
        self.conn.execute(query_template, params)
        self._log_file_insertion(file_path)

    def load_parquet_to_table(self, parquet_path: pathlib.PosixPath, table_name: str,
                              columns: Optional[List[str]] = None):
        parquet_path_str = str(parquet_path)
        if self._skip_load(parquet_path_str):
            return

        logger.info(f"Loading Parquet from {parquet_path_str} into table {table_name}")
        column_str = ", ".join(columns) if columns else "*"
        self._load_data(
            parquet_path_str,
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT {column_str} FROM read_parquet(?)",
            (parquet_path_str,)
        )

    def append_parquet_to_table(self, parquet_path: str, table_name: str, columns: Optional[List[str]] = None):
        if self._skip_load(parquet_path):
            return

        logger.info(f"Appending Parquet from {parquet_path} into table {table_name}")
        column_str = ", ".join(columns) if columns else "*"
        self._load_data(
            parquet_path,
            f"INSERT INTO {table_name} SELECT {column_str} FROM read_parquet(?)",
            (parquet_path,)
        )

    def load_csv_to_table(self, csv_path: str, table_name: str, separator: str = CONFIG.CSV_IMPORT_DELIM_STYLE):
        if self._skip_load(csv_path):
            return

        logger.info(f"Loading CSV from {csv_path} into table {table_name}")
        self._load_data(
            csv_path,
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto(?, delim=?)",
            (csv_path, separator)
        )

    def query(self, query_str: str) -> pl.DataFrame:
        logger.info(f"Executing query: {query_str}")
        return self.conn.execute(query_str).pl()

    def close(self):
        self.conn.close()
        logger.info("DuckDB connection closed.")
