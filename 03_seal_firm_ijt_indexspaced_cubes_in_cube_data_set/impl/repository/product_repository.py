import logging

import polars as pl

from impl.db.datasource import DuckDBDataSource
from impl.db.simple_sql_base_query_builder import SimpleSQLBaseQueryBuilder
from impl.repository.base.abc_repository import AbstractBaseRepository
from impl.repository.offers_repository import DEFAULT_OFFERS_TABLE

logger = logging.getLogger(__name__)

DEFAULT_PRODUCTS_TABLE = 'products'


class ProductRepository(AbstractBaseRepository):

    def __init__(self, db_source: DuckDBDataSource, table_name: str = DEFAULT_OFFERS_TABLE):
        super().__init__(db_source, table_name)

    def fetch_product_by_id(self, product_id: str) -> pl.DataFrame:
        """
        Fetch a product by its ID from the products table.

        Parameters:
        product_id (str): The product ID to filter by.

        Returns:
        pl.DataFrame: A Polars DataFrame containing the product details.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select('*')
            .where(f"produkt_id = '{product_id}'")
            .build()
        )
        return self.db_source.queryAsPl(query)
