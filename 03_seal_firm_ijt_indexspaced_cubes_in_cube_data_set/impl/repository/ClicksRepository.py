import polars as pl
from impl.db.querybuilder import QueryBuilder
from impl.db.datasource import DuckDBDataSource
import logging

from impl.repository.BaseRepository import BaseRepository

logger = logging.getLogger(__name__)

DEFAULT_CLICKS_TABLE = 'clicks'


class ClicksRepository(BaseRepository):
    def __init__(self,
                 db_source: DuckDBDataSource,
                 table_name: str = DEFAULT_CLICKS_TABLE):
        super().__init__(db_source, table_name)
        self.db_source = db_source

    def fetch_top_products_by_clicks(
            self,
            retailer: str,
            start_time_unix: int,
            end_time_unix: int,
            limit: int
    ) -> pl.DataFrame:
        query = (
            QueryBuilder(self.table_name)
            .select(['produkt_id', 'COUNT(*) AS total_clicks'])
            .where(f"haendler_bez = '{retailer}'")
            .where(f"timestamp >= {start_time_unix} AND timestamp <= {end_time_unix}")
            .group_by('produkt_id')
            .order_by('total_clicks', ascending=False)
            .limit(limit)
            .build()
        )
        return self.db_source.query(query)
