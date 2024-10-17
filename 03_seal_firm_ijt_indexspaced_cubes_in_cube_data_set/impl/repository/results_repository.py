import logging

import polars as pl

from impl.db.datasource import DuckDBDataSource
from impl.db.simple_sql_base_query_builder import SimpleSQLBaseQueryBuilder
from impl.repository.base.abc_repository import AbstractBaseRepository

logger = logging.getLogger(__name__)

DEFAULT_RESULTS_TABLE = 'results'


class ResultsDataRepository(AbstractBaseRepository):
    def fetch_all(self) -> pl.DataFrame:
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select("*")
            .build()
        )
        return self.db_source.queryAsPl(query)

    def __init__(self, db_source: DuckDBDataSource, table_name: str = DEFAULT_RESULTS_TABLE):
        super().__init__(db_source, table_name)

    def fetch_distinct_haendler_bez(self) -> pl.DataFrame:
        """
        Fetch distinct haendler_bez (which corresponds to j's).

        Returns:
        pl.DataFrame: A Polars DataFrame containing the distinct haendler_bez.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select("haendler_bez")
            .distinct()
            .build()
        )
        return self.db_source.queryAsPl(query)

    def fetch_distinct_produkt_ids(self) -> pl.DataFrame:
        """
        Fetch distinct produkt_ids (which corresponds to i's).

        Returns:
        pl.DataFrame: A Polars DataFrame containing the distinct produkt_ids.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select("produkt_id")
            .distinct()
            .build()
        )
        return self.db_source.queryAsPl(query)

    def fetch_distinct_t(self) -> pl.DataFrame:
        """
        Fetch distinct t values.

        Returns:
        pl.DataFrame: A Polars DataFrame containing the distinct t values.
        """
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select("zeit_t")
            .distinct()
            .build()
        )
        return self.db_source.queryAsPl(query)

    def get_row_count(self) -> int:
        query = (
            SimpleSQLBaseQueryBuilder(self.table_name)
            .select("COUNT(*)")
            .distinct()
            .build()
        )
        return self.db_source.query(query).type(int)
