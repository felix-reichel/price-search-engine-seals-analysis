import logging

from tqdm import tqdm

from CONFIG import FILTERED_HAENDLER_BEZ
from impl.db.duckdb_data_source import DuckDBDataSource
from impl.db.query_builder import QueryBuilder

import polars as pl

logger = logging.getLogger(__name__)


angebot_parquet_files = [
    '/scratch0/zieg/MeJ_Tests.d/angebot_06_10.pq/*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_11_14.pq/*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_15_19.pq/*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_20_24.pq/*.parquet'
]

clicks_parquet_file = '/scratch0/zieg/MeJ_Tests.d/clicks.pq/*.parquet'


def prepare_angebot_table(db: DuckDBDataSource):
    db.query("""
        CREATE TABLE IF NOT EXISTS angebot (
            produkt_id BIGINT,
            haendler_bez STRING,
            dtimebegin BIGINT,
            dtimeend BIGINT
        );
    """)


def prepare_clicks_table(db: DuckDBDataSource):
    db.query("""
        CREATE TABLE IF NOT EXISTS clicks (
            produkt_id BIGINT,
            haendler_bez STRING,
            click_timestamp BIGINT
        );
    """)


def load_relevant_angebot_data(db: DuckDBDataSource, allowed_firms, columns=None):

    if columns is None:
        columns = ['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend']

    total_files = len(angebot_parquet_files)

    with tqdm(total=total_files, desc="Loading Angebot Data", unit="file", ncols=100) as pbar:

        for file_path in angebot_parquet_files:

            logger.info(f"Loading Angebot data from {file_path}")

            db.load_parquet_to_table(
                file_path,
                'angebot_temp',
                columns=columns)

            insert_query = QueryBuilder('angebot_temp') \
                .insert_into('angebot') \
                .select('*') \
                .where(f"haendler_bez IN {tuple(allowed_firms)}") \
                .build()

            db.query(insert_query)
            pbar.update(1)

    logger.info("All relevant Angebot data loaded.")


def load_relevant_clicks_data(db: DuckDBDataSource, allowed_firms, columns=None):

    if columns is None:
        columns = ['produkt_id', 'haendler_bez', 'timestamp']

    logger.info(f"Loading Clicks data from {clicks_parquet_file}")

    db.load_parquet_to_table(
        clicks_parquet_file,
        'clicks_temp',
        columns=columns)

    insert_query = QueryBuilder('clicks_temp') \
        .insert_into('clicks') \
        .select('*') \
        .where(f"haendler_bez IN {tuple(allowed_firms)}") \
        .build()

    db.query(insert_query)

    logger.info("All relevant Clicks data loaded.")


def main(allowed_firms):
    db = DuckDBDataSource('/scratch0/zieg/MeJ_Tests.d/MeJ_Tests.db')

    prepare_angebot_table(db)
    prepare_clicks_table(db)

    load_relevant_angebot_data(db, allowed_firms)
    load_relevant_clicks_data(db, allowed_firms)

    logger.info("Verifying Angebot table record count.")
    result_angebot = db.query("SELECT COUNT(*) AS count FROM angebot;")
    logger.info(f"Angebot table record count: {result_angebot.iloc[0]['count']}")

    logger.info("Verifying Clicks table record count.")
    result_clicks = db.query("SELECT COUNT(*) AS count FROM clicks;")
    logger.info(f"Clicks table record count: {result_clicks.iloc[0]['count']}")

    db.close()


if __name__ == "__main__":
    main(allowed_firms=pl.read_csv(FILTERED_HAENDLER_BEZ))
