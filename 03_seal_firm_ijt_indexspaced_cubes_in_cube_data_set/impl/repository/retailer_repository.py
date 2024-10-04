import logging

import polars as pl

from impl.db.datasource import DuckDBDataSource
from impl.db.querybuilder import QueryBuilder
from impl.repository.base.base_repository import BaseRepository

logger = logging.getLogger(__name__)

DEFAULT_RETAILER_TABLE = 'retailer'


class RetailerRepository(BaseRepository):
    def __init__(self, db_source: DuckDBDataSource, table_name: str = DEFAULT_RETAILER_TABLE):
        super().__init__(db_source, table_name)

    def add_data(self, data: pl.DataFrame):
        """
        Add data to the retailers table.

        Parameters:
        data (pl.DataFrame): A Polars DataFrame containing the data to insert into the table.
        """
        logger.info(f"Adding data to {self.table_name}")
        self.db_source.insert_df(self.table_name, data)

    def fetch_all_retailers(self) -> pl.DataFrame:
        """
        Fetch all retailers from the table.

        Returns:
        pl.DataFrame: A Polars DataFrame containing all the retailers.
        """
        query = (
            QueryBuilder(self.table_name)
            .select('*')
            .build()
        )
        return self.db_source.queryAsPl(query)

    def fetch_retailer_by_id(self, retailer_id: str) -> pl.DataFrame:
        """
        Fetch a specific retailer by its ID from the table.

        Parameters:
        retailer_id (str): The retailer ID to filter by.

        Returns:
        pl.DataFrame: A Polars DataFrame containing the retailer's details.
        """
        query = (
            QueryBuilder(self.table_name)
            .select('*')
            .where(f"haendler_bez = '{retailer_id}'")
            .build()
        )
        return self.db_source.queryAsPl(query)
