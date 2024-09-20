import polars as pl
from impl.db.querybuilder import QueryBuilder
from impl.db.datasource import DuckDBDataSource
import logging

from impl.repository.BaseRepository import BaseRepository

logger = logging.getLogger(__name__)

DEFAULT_SEAL_CHANGE_FIRMS_TABLE = 'seal_change_firms'


class SealChangeFirmsDataRepository(BaseRepository):
    def __init__(self,
                 db_source: DuckDBDataSource,
                 table_name: str = DEFAULT_SEAL_CHANGE_FIRMS_TABLE):
        super().__init__(db_source, table_name)
        self.db_source = db_source
        self.table_name = table_name

    def fetch_seal_change_firms_by_retailer_name(
            self,
            retailer_name: str
    ) -> pl.DataFrame:
        query = (
            QueryBuilder(self.table_name)
            .select('*')
            .where(f"matched_haendler_bez = '{retailer_name}'")
            .build()
        )
        return self.db_source.query(query)

    def fetch_all(
            self
    ) -> pl.DataFrame:
        query = (
            QueryBuilder(self.table_name)
            .select('*')
            .build()
        )
        return self.db_source.query(query)
