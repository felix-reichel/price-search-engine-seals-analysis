import polars as pl
from impl.db.querybuilder import QueryBuilder
from impl.db.datasource import DuckDBDataSource
import logging

logger = logging.getLogger(__name__)

DEFAULT_OFFERS_TABLE = 'angebot'


class OffersRepository():
    def __init__(
            self,
            db_source: DuckDBDataSource,
            table_name: str = DEFAULT_OFFERS_TABLE
    ):
        self.db_source = db_source
        self.table_name = table_name

    # TODO: Refactor ME
    def fetch_offered_weeks(
            self,
            product_id: str,
            firm_id: str,
            time_range_start: int,
            time_range_end: int
    ) -> pl.DataFrame:
        query = (
            QueryBuilder(self.table_name)
            .select(['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend'])
            .build_where_clause(product_id, firm_id, time_range_start, time_range_end)
            .build()
        )
        return self.db_source.query(query)

    def fetch_counterfactual_firms(
            self,
            product_id: str,
            seal_date_unix: int
    ) -> pl.DataFrame:
        query = (
            QueryBuilder(self.table_name)
            .select('haendler_bez')
            .distinct()
            .where(f"produkt_id = '{product_id}'")
            .where(f"dtimebegin <= {seal_date_unix} AND dtimeend >= {seal_date_unix}")
            .build()
        )
        return self.db_source.query(query)

    def fetch_product_offer_data(
            self,
            product_id: str,
            retailer: str,
            offer_start_unix: int,
            offer_end_unix: int
    ) -> pl.DataFrame:
        query = (
            QueryBuilder(self.table_name)
            .select(['dtimebegin', 'dtimeend'])
            .where(f"produkt_id = '{product_id}'")
            .where(f"haendler_bez = '{retailer}'")
            .where(f"dtimebegin <= {offer_end_unix} AND dtimeend >= {offer_start_unix}")
            .build()
        )
        return self.db_source.query(query)

    def fetch_random_products(
            self,
            retailer: str,
            observation_start_unix: int,
            observation_end_unix: int
    ) -> pl.DataFrame:
        query = (
            QueryBuilder(self.table_name)
            .select('produkt_id')
            .distinct()
            .where(f"haendler_bez = '{retailer}'")
            .where(f"dtimebegin <= '{observation_end_unix}' AND dtimeend >= '{observation_start_unix}'")
            .build()
        )
        return self.db_source.query(query)
