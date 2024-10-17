import logging

import polars as pl

from impl.db.datasource import DuckDBDataSource
from impl.db.simple_sql_base_query_builder import SimpleSQLBaseQueryBuilder
from impl.repository.base.abc_repository import AbstractBaseRepository

logger = logging.getLogger(__name__)

DEFAULT_OFFERS_TABLE = 'angebot'


class OffersRepository(AbstractBaseRepository):
    def __init__(self, db_source: DuckDBDataSource, table_name: str = DEFAULT_OFFERS_TABLE):
        super().__init__(db_source, table_name)

    def fetch_offered_weeks(self, product_id: str, firm_id: str, time_range_start: int, time_range_end: int) -> pl.DataFrame:
        """
        Fetch the weeks during which a product was offered by a firm within a specific time range.

        Parameters:
        product_id (str): The product ID to filter by.
        firm_id (str): The firm (retailer) ID to filter by.
        time_range_start (int): The start of the time range (Unix timestamp).
        time_range_end (int): The end of the time range (Unix timestamp).

        Returns:
        pl.DataFrame: A Polars DataFrame containing the offered weeks.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select(['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend'])
            .build_ijt_where_clause(product_id, firm_id, time_range_start, time_range_end)
            .build()
        )
        return self.db_source.queryAsPl(query)

    def fetch_counterfactual_firms(self, product_id: str, seal_date_unix: int) -> pl.DataFrame:
        """
        Fetch firms that offered a product around the time of a seal change.

        Parameters:
        product_id (str): The product ID to filter by.
        seal_date_unix (int): The Unix timestamp representing the seal date.

        Returns:
        pl.DataFrame: A Polars DataFrame containing the distinct firms.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select('haendler_bez')
            .distinct()
            .where(f"produkt_id = '{product_id}'")
            .where(f"dtimebegin <= {seal_date_unix} AND dtimeend >= {seal_date_unix}")
            .build()
        )
        return self.db_source.queryAsPl(query)

    def fetch_product_offer_data(self, product_id: str, retailer: str, offer_start_unix: int, offer_end_unix: int) -> pl.DataFrame:
        """
        Fetch offer data for a product from a specific retailer within a time range.

        Parameters:
        product_id (str): The product ID to filter by.
        retailer (str): The retailer (firm) to filter by.
        offer_start_unix (int): The Unix timestamp representing the offer start time.
        offer_end_unix (int): The Unix timestamp representing the offer end time.

        Returns:
        pl.DataFrame: A Polars DataFrame containing the offer data.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select(['dtimebegin', 'dtimeend'])
            .where(f"produkt_id = '{product_id}'")
            .where(f"haendler_bez = '{retailer}'")
            .where(f"dtimebegin <= {offer_end_unix} AND dtimeend >= {offer_start_unix}")
            .build()
        )
        return self.db_source.queryAsPl(query)

    def fetch_random_products(self, retailer: str, observation_start_unix: int, observation_end_unix: int) -> pl.DataFrame:
        """
        Fetch distinct random products offered by a specific retailer within a time range.

        Parameters:
        retailer (str): The retailer (firm) to filter by.
        observation_start_unix (int): The Unix timestamp representing the observation start time.
        observation_end_unix (int): The Unix timestamp representing the observation end time.

        Returns:
        pl.DataFrame: A Polars DataFrame containing the distinct product IDs.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select('produkt_id')
            .distinct()
            .where(f"haendler_bez = '{retailer}'")
            .where(f"dtimebegin <= {observation_end_unix} AND dtimeend >= {observation_start_unix}")
            .build()
        )
        return self.db_source.queryAsPl(query)
