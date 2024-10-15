import logging

import polars as pl

from impl.db.datasource import DuckDBDataSource
from impl.db.simple_sql_base_query_builder import SimpleSQLBaseQueryBuilder
from impl.repository.base.abc_repository import AbstractBaseRepository

logger = logging.getLogger(__name__)

DEFAULT_SEAL_CHANGE_FIRMS_TABLE = 'seal_change_firms'


class SealChangeFirmsDataRepository(AbstractBaseRepository):
    def __init__(self, db_source: DuckDBDataSource, table_name: str = DEFAULT_SEAL_CHANGE_FIRMS_TABLE):
        super().__init__(db_source, table_name)

    def fetch_seal_change_firms_by_retailer_name(self, retailer_name: str) -> pl.DataFrame:
        """
        Fetch seal change firms by the matched retailer name.

        Parameters:
        retailer_name (str): The retailer's name to filter by.

        Returns:
        pl.DataFrame: A Polars DataFrame containing the matching seal change firms.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select('*')
            .where(f"matched_haendler_bez = '{retailer_name}'")
            .build()
        )
        return self.db_source.queryAsPl(query)

    def fetch_all(self) -> pl.DataFrame:
        """
        Fetch all seal change firms.

        Returns:
        pl.DataFrame: A Polars DataFrame containing all seal change firms.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select('*')
            .build()
        )
        return self.db_source.queryAsPl(query)
