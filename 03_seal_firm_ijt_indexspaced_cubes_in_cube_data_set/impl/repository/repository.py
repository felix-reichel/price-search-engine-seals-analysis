import polars as pl
from impl.db.query_builder import QueryBuilder
from impl.db.duckdb_data_source import DuckDBDataSource
import logging

logger = logging.getLogger(__name__)

ANGEBOTE_TABLE_NAME = 'angebot'
CLICKS_TABLE_NAME = 'clicks'


class ProductRepository:
    def __init__(self, db: DuckDBDataSource):
        self.db = db

    def get_offered_weeks(self, prod_id, firm_id, unix_time_spells_from, unix_time_spells_to):
        """
        Fetch the offered weeks for a product and firm within the specified time range.
        """
        query = QueryBuilder(ANGEBOTE_TABLE_NAME) \
            .select(['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend']) \
            .build_where_clause_i_j_t(prod_id, firm_id, unix_time_spells_from, unix_time_spells_to) \
            .build()

        result = self.db.query(query)
        return pl.DataFrame(result)

    def get_counterfactual_firms(self, product_id, seal_date_unix):
        """
        Fetch the firms offering a product around the seal date.
        """
        query = QueryBuilder(ANGEBOTE_TABLE_NAME) \
            .select('haendler_bez') \
            .where(f"produkt_id = '{product_id}'") \
            .where(f"dtimebegin <= {seal_date_unix} AND dtimeend >= {seal_date_unix}") \
            .build()

        result = self.db.query(query)
        return pl.DataFrame(result)

    def get_top_n_products_by_clicks(self, haendler_bez, observation_start_unix, observation_end_unix, top_n):
        """
        Fetch the top N products based on clicks within a specified observation period.
        """
        query = QueryBuilder(CLICKS_TABLE_NAME) \
            .select(['produkt_id', 'COUNT(*) AS count']) \
            .where(f"haendler_bez = '{haendler_bez}'") \
            .where(f"timestamp >= {observation_start_unix} AND timestamp <= {observation_end_unix}") \
            .group_by('produkt_id') \
            .order_by('count', ascending=False) \
            .limit(top_n) \
            .build()

        result = self.db.query(query)
        return pl.DataFrame(result)

    def get_product_offer_data(self, produkt_id, haendler_bez, offered_period_start_unix, offered_period_end_unix):
        """
        Fetch the offer data for a product by a firm in the given period.
        """
        query = QueryBuilder(ANGEBOTE_TABLE_NAME) \
            .select(['dtimebegin', 'dtimeend']) \
            .where(f"produkt_id = '{produkt_id}'") \
            .where(f"haendler_bez = '{haendler_bez}'") \
            .where(f"dtimebegin <= {offered_period_end_unix} AND dtimeend >= {offered_period_start_unix}") \
            .build()

        result = self.db.query(query)
        return pl.DataFrame(result)

    def get_random_products(self, haendler_bez, observation_start_unix, observation_end_unix):
        """
        Fetch products available in a given time window for a firm.
        """
        query = QueryBuilder(ANGEBOTE_TABLE_NAME) \
            .select('produkt_id') \
            .where(f"haendler_bez = '{haendler_bez}'") \
            .where(f"dtimebegin <= {observation_end_unix} AND dtimeend >= {observation_start_unix}") \
            .build()

        result = self.db.query(query)
        return pl.DataFrame(result)
