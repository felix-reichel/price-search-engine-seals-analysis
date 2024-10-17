import logging

import polars as pl

from impl.db.datasource import DuckDBDataSource
from impl.db.simple_sql_base_query_builder import SimpleSQLBaseQueryBuilder
from impl.repository.base.abc_repository import AbstractBaseRepository

logger = logging.getLogger(__name__)

DEFAULT_CLICKS_TABLE = 'clicks'


class ClicksRepository(AbstractBaseRepository):

    def __init__(self, db_source: DuckDBDataSource, table_name: str = DEFAULT_CLICKS_TABLE):
        super().__init__(db_source, table_name)

    def fetch_top_products_by_clicks(
        self,
        retailer: str,
        start_time_unix: int,
        end_time_unix: int,
        limit: int
    ) -> pl.DataFrame:
        """
        Fetch the top products by clicks for a given retailer and time range.

        Parameters:
        retailer (str): The retailer's name (haendler_bez).
        start_time_unix (int): Start of the time range (Unix timestamp).
        end_time_unix (int): End of the time range (Unix timestamp).
        limit (int): The maximum number of products to return.

        Returns:
        pl.DataFrame: A Polars DataFrame containing the top products and their total clicks.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select(['produkt_id', 'COUNT(*) AS total_clicks'])
            .where(f"haendler_bez = '{retailer}'")
            .where(f"timestamp >= {start_time_unix} AND timestamp <= {end_time_unix}")
            .group_by('produkt_id')
            .order_by('total_clicks', ascending=False)
            .limit(limit)
            .build()
        )
        return self.db_source.queryAsPl(query)
