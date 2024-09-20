import polars as pl
from impl.db.querybuilder import QueryBuilder
from impl.db.datasource import DuckDBDataSource
import logging

from impl.repository.BaseRepository import BaseRepository

logger = logging.getLogger(__name__)

DEFAULT_FILTERED_TABLE = 'filtered_haendler_bez'


class FilteredRetailerNamesRepository(BaseRepository):
    def __init__(self, db_source: DuckDBDataSource, table_name: str = DEFAULT_FILTERED_TABLE):
        super().__init__(db_source, table_name)
        self.db_source = db_source
        self.table_name = table_name

    def fetch_all_filtered_retailers(self) -> pl.DataFrame:
        query = (
            QueryBuilder(self.table_name)
            .select('*')
            .build()
        )
        return self.db_source.query(query)
