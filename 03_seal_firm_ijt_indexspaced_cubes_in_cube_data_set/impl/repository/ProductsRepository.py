import polars as pl
from impl.db.querybuilder import QueryBuilder
from impl.db.datasource import DuckDBDataSource
import logging

from impl.repository.BaseRepository import BaseRepository

logger = logging.getLogger(__name__)

DEFAULT_PRODUCTS_TABLE = 'products'


class ProductDataRepository(BaseRepository):
    def __init__(self, db_source: DuckDBDataSource, table_name: str = DEFAULT_PRODUCTS_TABLE):
        super().__init__(db_source, table_name)
        self.db_source = db_source
        self.table_name = table_name

    def fetch_product_by_id(
            self,
            product_id: str
    ) -> pl.DataFrame:
        query = (
            QueryBuilder(self.table_name)
            .select('*')
            .where(f"produkt_id = '{product_id}'")
            .build()
        )
        return self.db_source.query(query)
