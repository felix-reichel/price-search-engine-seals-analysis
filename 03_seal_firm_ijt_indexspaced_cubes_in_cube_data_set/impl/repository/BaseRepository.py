import polars as pl
import logging
from impl.db.datasource import DuckDBDataSource

logger = logging.getLogger(__name__)


class BaseRepository:
    def __init__(self,
                 db_source: DuckDBDataSource,
                 table_name: str):
        self.db_source = db_source
        self.table_name = table_name

    def add_data(self,
                 data: pl.DataFrame):
        self.db_source.insert_df(self.table_name, data)

    def fetch_all(self) -> pl.DataFrame:
        raise NotImplementedError()
