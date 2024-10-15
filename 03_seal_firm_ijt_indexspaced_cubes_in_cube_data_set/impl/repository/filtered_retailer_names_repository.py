import logging

import polars as pl

from impl.db.datasource import DuckDBDataSource
from impl.db.simple_sql_base_query_builder import SimpleSQLBaseQueryBuilder
from impl.repository.base.abc_repository import AbstractBaseRepository

logger = logging.getLogger(__name__)

DEFAULT_FILTERED_TABLE = 'filtered_haendler_bez'


class FilteredRetailerNamesRepository(AbstractBaseRepository):
    def fetch_all(self) -> pl.DataFrame:
        return self.fetch_all_filtered_retailers()

    def __init__(self, db_source: DuckDBDataSource, table_name: str = DEFAULT_FILTERED_TABLE):
        super().__init__(db_source, table_name)

    def fetch_all_filtered_retailers(self) -> pl.DataFrame:
        """
        Fetch all filtered retailer names from the table.

        Returns:
        pl.DataFrame: A Polars DataFrame containing all the filtered retailer names.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select('*')
            .build()
        )
        return self.db_source.queryAsPl(query)
