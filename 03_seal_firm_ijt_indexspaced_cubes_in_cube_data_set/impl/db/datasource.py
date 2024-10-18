import logging
import pathlib
import time
from datetime import datetime
from typing import List, Optional

import duckdb
import polars as pl

import CONFIG
from ApplicationThreadConfig import ApplicationThreadConfig
from TABLES_CONFIG import TABLES_CONFIG
from impl.singleton import Singleton

logger = logging.getLogger(__name__)


class DuckDBDataSource(Singleton):
    def __init__(self,
                 db_path: str = CONFIG.DUCKDB_PATH,
                 threads: int = CONFIG.MAX_DUCKDB_THREADS,
                 memory_limit: str = CONFIG.DUCKDB_MEMORY_LIMIT,
                 tables_config: dict = TABLES_CONFIG,
                 bypass_singleton=False,
                 bypass_application_thread_config=False):
        """
        Initialize the DuckDB connection and configure threads.

        Parameters:
        db_path (str): The path to the DuckDB database file.
        threads (int): The number of threads DuckDB should use.
        bypass_singleton (bool): Whether to bypass Singleton for test purposes.
        """
        if bypass_singleton or not hasattr(self, 'conn'):
            # Create DuckDB connection if it doesn't exist or bypassing singleton
            logger.info(f"Establishing new DuckDB connection with db_path={db_path}")
            self.conn = duckdb.connect(db_path, config={'threads': threads,
                                                        'memory_limit': memory_limit})

            if not bypass_application_thread_config:
                # Apply thread configuration
                ApplicationThreadConfig.apply_thread_config(self)

            # Log DuckDB config for debugging
            self.log_duckdb_config()

            # Initialize file log table
            self.initialize_file_log_table()

            # Set TABLES_CONFIG dict
            self.tables_config = tables_config

            # Set allocator_background_threads
            self.conn.execute("SET allocator_background_threads=true;")

    # https://duckdb.org/docs/configuration/overview.html
    def log_duckdb_config(self):
        """
        Log all DuckDB configuration settings using SELECT * FROM duckdb_settings().
        """
        settings = self.conn.execute("SELECT * FROM duckdb_settings() WHERE name = 'threads';").fetchall()

        for setting in settings:
            # Each setting contains (name, value, description)
            logger.info(f"DuckDB Config - {setting[0]}: {setting[1]} - {setting[2]}")

    def initialize_file_log_table(self):
        """
        Create a file log table if it does not exist to track file insertions.
        """
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS file_log (
                file_name TEXT PRIMARY KEY,
                insert_timestamp TIMESTAMP
            )
        """)
        logger.info("Initialized file_log table.")

    def _has_file_been_inserted(self, file_name: str) -> bool:
        """
        Check if the file has already been inserted into the database.

        Parameters:
        file_name (str): The name of the file to check.

        Returns:
        bool: True if the file has been inserted, otherwise False.
        """
        result = self.conn.execute(
            "SELECT COUNT(*) AS count FROM file_log WHERE file_name = ?",
            (file_name,)
        ).fetchone()
        return result[0] > 0

    def _log_file_insertion(self, file_name: str):
        """
        Log the insertion of a file with the current timestamp.

        Parameters:
        file_name (str): The name of the file being inserted into the log.
        """
        insert_time = datetime.now()
        self.conn.execute(
            "INSERT INTO file_log (file_name, insert_timestamp) VALUES (?, ?)",
            (file_name, insert_time)
        )
        logger.info(f"Logged file insertion: {file_name} at {insert_time}")

    def _skip_load(self, file_path: str) -> bool:
        """
        Skip loading the file if it has already been inserted into the database.

        Parameters:
        file_path (str): The path of the file to check for skipping.

        Returns:
        bool: True if the file should be skipped, otherwise False.
        """
        if self._has_file_been_inserted(file_path):
            logger.info(f"File {file_path} has already been inserted. Skipping load.")
            return True
        return False

    def _load_data(self,
                   file_path: str,
                   query_template: str,
                   params: tuple):
        """
        Execute the query to load data from a file and log the file insertion.

        Parameters:
        file_path (str): The path of the file being loaded.
        query_template (str): The SQL query template to execute.
        params (tuple): The parameters to pass into the query.
        """
        logger.info(f"Loading data from {file_path}")
        self.conn.execute(query_template, params)
        self._log_file_insertion(file_path)

    def load_parquet_to_table(self,
                              parquet_path: pathlib.PosixPath,
                              table_name: str,
                              columns: Optional[List[str]] = None):
        """
        Load a Parquet file into a table. Optionally specify columns to load.

        Parameters:
        parquet_path (pathlib.PosixPath): The path of the Parquet file.
        table_name (str): The name of the table to load the data into.
        columns (List[str], optional): A list of column names to load. Loads all columns if None.
        """
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

    def gz_load_filtered_parquet_to_table(self,
                                          parquet_path: pathlib.PosixPath,
                                          table_name: str,
                                          columns: Optional[List[str]] = None):
        parquet_path_str = str(parquet_path)
        if self._skip_load(parquet_path_str):
            return

        logger.info(f"Loading Parquet from {parquet_path_str} into table {table_name}")
        column_str = ", ".join(columns) if columns else "*"
        self._load_data(
            parquet_path_str,
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT {column_str} FROM read_parquet(?) pq"
            f" WHERE EXISTS ( SELECT 1 FROM filtered_haendler_bez WHERE haendler_bez = pq.haendler_bez )",
            (parquet_path_str,)
        )

    def append_parquet_to_table(self,
                                parquet_path: str,
                                table_name: str,
                                columns: Optional[List[str]] = None):
        """
        Append data from a Parquet file to an existing table. Optionally specify columns.

        Parameters:
        parquet_path (str): The path of the Parquet file.
        table_name (str): The name of the table to append the data to.
        columns (List[str], optional): A list of column names to append. Appends all columns if None.
        """
        if self._skip_load(parquet_path):
            return

        logger.info(f"Appending Parquet from {parquet_path} into table {table_name}")
        column_str = ", ".join(columns) if columns else "*"
        self._load_data(
            parquet_path,
            f"INSERT INTO {table_name} SELECT {column_str} FROM read_parquet(?)",
            (parquet_path,)
        )

    def gz_append_filtered_parquet_to_table(self,
                                            parquet_path: str,
                                            table_name: str,
                                            columns: Optional[List[str]] = None):
        if self._skip_load(parquet_path):
            return

        logger.info(f"Appending Filtered Parquet from {parquet_path} into table {table_name}")
        column_str = ", ".join(columns) if columns else "*"
        self._load_data(
            parquet_path,
            f"INSERT INTO {table_name} SELECT {column_str} FROM read_parquet(?) pq "
            f"WHERE EXISTS ( SELECT 1 FROM filtered_haendler_bez WHERE haendler_bez = pq.haendler_bez )",
            (parquet_path,)
        )

    def load_csv_to_table(self,
                          csv_path: str,
                          table_name: str,
                          separator: str = CONFIG.CSV_IMPORT_DELIM_STYLE):
        """
        Load data from a CSV file into a table.

        Parameters:
        csv_path (str): The path of the CSV file.
        table_name (str): The name of the table to load the data into.
        separator (str): The delimiter used in the CSV file.
        """
        if self._skip_load(csv_path):
            return

        logger.info(f"Loading CSV from {csv_path} into table {table_name}")
        self._load_data(
            csv_path,
            f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_csv_auto(?, delim=?)",
            (csv_path, separator)
        )

    def free_up_table_and_manipulate_file_logs(self, table_name: str):
        """
        Frees up memory by deleting and dropping the specified table, and removes any related file logs.

        Parameters:
        table_name (str): The name of the table to be deleted and dropped.
        """
        self.conn.execute("BEGIN;")
        try:
            self.conn.execute(f"DELETE FROM file_log WHERE file_name LIKE '{table_name}%'")
            logger.info(f"File log entries for '{table_name}' have been deleted.")

            self.conn.execute(f"DELETE FROM {table_name}")
            logger.info(f"Data from table '{table_name}' has been deleted.")

            self.conn.execute(f"DROP TABLE IF EXISTS {table_name};")
            logger.info(f"Table '{table_name}' dropped to free memory.")

            self.conn.execute("COMMIT;")
            logger.info("Transaction committed successfully.")

        except duckdb.CatalogException as e:
            self.conn.execute("ROLLBACK;")
            logger.error(f"Error occurred, rolling back transaction: {e}")

    def drop_table_if_needed(self, table_name: str, config: dict):
        """
        Drops the table based on configuration such as row limit and cache duration.
        """
        drop_after_use = config.get("drop_after_use", False)
        row_limit = config.get("row_limit", None)
        cache_duration = config.get("cache_duration", None)

        # Check row limit
        if row_limit:
            row_count = self.conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
            if row_count > row_limit:
                logger.info(f"Row limit exceeded for {table_name}. Dropping table.")
                self.free_up_table_and_manipulate_file_logs(table_name)
                return

        # TODO: Check cache duration
        if cache_duration:
            table_info = self.conn.execute(f"PRAGMA table_info('{table_name}');").fetchall()
            if table_info:
                last_modified_time = time.time()  # TODO: Use actual last modified time if available
                current_time = time.time()
                elapsed_time = (current_time - last_modified_time) / 60  # convert to minutes
                if elapsed_time > cache_duration:
                    logger.info(f"Cache duration exceeded for {table_name}. Dropping table.")
                    self.free_up_table_and_manipulate_file_logs(table_name)
                    return

        if drop_after_use:
            logger.info(f"Dropping table {table_name} based on drop_after_use configuration.")
            self.free_up_table_and_manipulate_file_logs(table_name)

    def process_tables(self):
        """
        Loops through the tables in the configuration and processes them.
        """
        for key, config in self.tables_config.items():
            table_name = config.get("folder_name", config.get("file_name"))
            if not table_name:
                logger.error(f"Table name not found in config for {key}")
                continue

            if self.table_exists(table_name):
                logger.info(f"Processing table: {table_name}")
                self.drop_table_if_needed(table_name, config)
            else:
                logger.info(f"Table {table_name} does not exist in the database.")

    def table_exists(self, table_name: str) -> bool:
        """
        Checks if a table exists in the DuckDB database using PRAGMA.
        """
        try:
            tables = self.conn.execute("PRAGMA show_tables;").fetchall()
            return (table_name,) in tables
        except duckdb.CatalogException:
            return False

    def get_database_size(self):
        """
        Retrieves database size using DuckDB's PRAGMA database_size.
        """
        size_info = self.conn.execute("CALL pragma_database_size();").fetchall()
        for row in size_info:
            logger.info(f"Database Name: {row[0]}, Size: {row[1]}, Used Blocks: {row[4]}")
        return size_info

    def get_table_info(self, table_name: str):
        """
        Retrieves detailed table information using DuckDB's PRAGMA storage_info.
        """
        storage_info = self.conn.execute(f"PRAGMA storage_info('{table_name}');").fetchall()
        for row in storage_info:
            logger.info(f"Table: {table_name}, Column: {row[1]}, Compression: {row[7]}")
        return storage_info

    def queryAsPl(self, query_str: str) -> pl.DataFrame:
        """
        Execute a query and return the result as a Polars DataFrame.

        Parameters:
        query_str (str): The SQL query to execute.

        Returns:
        pl.DataFrame: The result of the query as a Polars DataFrame.
        """
        logger.info(f"Executing query: {query_str}")
        return self.conn.execute(query_str).pl()

    def query(self, query_str: str):
        """
        Execute a query.

        Parameters:
        query_str (str): The SQL query to execute.

        Returns:
            duckdb.duckdb.DuckDBPyConnection
        """
        logger.info(f"Executing query: {query_str}")
        return self.conn.execute(query_str)

    def close(self):
        """
        Close the DuckDB connection.
        """
        if hasattr(self, 'conn') and self.conn:
            logger.info("Closing DuckDB connection.")
            self.conn.close()
            self.conn = None
        else:
            logger.warning("DuckDB connection was not established or already closed.")
