import logging
from abc import ABC

import polars as pl

from impl.db.datasource import DuckDBDataSource
from impl.singleton import Singleton

logger = logging.getLogger(__name__)


class AbstractBaseRepository(ABC, Singleton):
    """
    The AbstractBaseRepository class serves as an abstract base class for any repository
    that interacts with a specific table in the DuckDB database. It is designed
    to be inherited by concrete repository classes that implement specific data
    fetching logic.

    The class follows the Singleton pattern by inheriting from the Singleton base class,
    ensuring that only one instance of each repository exists throughout the application's
    lifecycle. This prevents unnecessary resource usage and improves efficiency.

    Attributes:
        db_source (DuckDBDataSource): The database connection source for DuckDB.
        table_name (str): The name of the table that the repository interacts with.
    """

    def __init__(self, db_source: DuckDBDataSource, table_name: str):
        """
        Initializes the AbstractBaseRepository with a database source and a table name.
        Ensures that both db_source and table_name are only set once, respecting
        the Singleton pattern.

        Parameters:
            db_source (DuckDBDataSource): The DuckDB database connection.
            table_name (str): The name of the table associated with the repository.
        """
        # Ensure db_source is only set once (Singleton behavior)
        if not hasattr(self, 'db_source'):
            self.db_source = db_source

        # Ensure table_name is only set once (Singleton behavior)
        if not hasattr(self, 'table_name'):
            self.table_name = table_name

    def _execute_query(self, query: str) -> pl.DataFrame:
        """
        Executes a given SQL query and returns the result as a Polars DataFrame.

        Parameters:
            query (str): The SQL query to be executed.

        Returns:
            pl.DataFrame: A Polars DataFrame containing the query result.
        """
        return self.db_source.queryAsPl(query_str=query)
