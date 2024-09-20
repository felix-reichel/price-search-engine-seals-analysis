import polars as pl
from impl.db.querybuilder import QueryBuilder
from impl.db.datasource import DuckDBDataSource
import logging

from impl.repository.BaseRepository import BaseRepository

logger = logging.getLogger(__name__)

DEFAULT_RETAILERS_TABLE = 'retailers'


class RetailersRepository(BaseRepository):
    def __init__(self,
                 db_source: DuckDBDataSource,
                 table_name: str = DEFAULT_RETAILERS_TABLE):
        super().__init__(db_source, table_name)
        self.db_source = db_source
        self.table_name = table_name

    def add_data(
            self,
            data: pl.DataFrame
    ):
        logger.info(f"Adding data to {self.table_name}")
        self.db_source.insert_df(self.table_name, data)

    def fetch_all_retailers(
            self
    ) -> pl.DataFrame:
        query = (
            QueryBuilder(self.table_name)
            .select('*')
            .build()
        )
        return self.db_source.query(query)

    def fetch_retailer_by_id(
            self,
            retailer_id: str
    ) -> pl.DataFrame:
        query = (
            QueryBuilder(self.table_name)
            .select('*')
            .where(f"haendler_bez = '{retailer_id}'")
            .build()
        )
        return self.db_source.query(query)
