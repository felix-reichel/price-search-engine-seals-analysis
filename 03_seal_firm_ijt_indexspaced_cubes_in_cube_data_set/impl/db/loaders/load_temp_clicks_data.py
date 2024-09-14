from tqdm import tqdm
from CONFIG import CLICKS_SCHEME, PARQUE_FILES_DIR, CLICKS_FOLDER
from impl.main import get_year_month_from_seal_date, generate_months_around_seal
from impl.db.query_builder import QueryBuilder
from impl.db.duckdb_data_source import DuckDBDataSource
import logging
import os

logger = logging.getLogger(__name__)


def load_relevant_click_data(
        db: DuckDBDataSource,
        seal_date,
        columns=None):
    if columns is None:
        columns = ['produkt_id', 'haendler_bez', 'timestamp']

    seal_year, seal_month = get_year_month_from_seal_date(seal_date)
    relevant_months = generate_months_around_seal(seal_year, seal_month)

    table_created = False
    total_files = len(relevant_months)

    with tqdm(total=total_files, desc="Loading Click Data", unit="file", ncols=100) as pbar:
        for month in relevant_months:
            file_name = CLICKS_SCHEME.format(year=seal_year, month=f"{month:02d}")
            file_path = PARQUE_FILES_DIR / file_name

            if os.path.isfile(file_path):
                if not table_created:
                    logger.info(f"Creating table from {file_path}")
                    db.load_parquet_to_table(file_path, 'clicks_temp', columns=columns)
                    table_created = True
                else:
                    logger.info(f"Appending data from {file_path}")
                    db.append_parquet_to_table(file_path, 'clicks_temp', columns=columns)

            pbar.update(1)

    if table_created:
        count_query = QueryBuilder('clicks_temp') \
            .select('COUNT(*) AS count') \
            .build()

        count_result = db.query(count_query)
        count = count_result.iloc[0]['count']

        logger.info(f"Total Click data rows loaded: {count}")
        return count

    else:
        logger.warning(f"No relevant Click data found for seal date {seal_date}.")
        return 0


def load_relevant_click_data_v2(db, seal_date):
    seal_year, seal_month = get_year_month_from_seal_date(seal_date)
    relevant_months = generate_months_around_seal(seal_year, seal_month)

    for month in relevant_months:
        file_name = CLICKS_SCHEME.format(year=seal_year, month=f"{month:02d}")
        file_path = PARQUE_FILES_DIR / CLICKS_FOLDER / file_name

        if os.path.isfile(file_path):
            logger.info(f"Loading Clicks data from {file_path}")
            db.load_parquet_to_table(file_path, 'clicks_temp')


def prepare_clicks_table(db):
    db.query(f"""
        CREATE TABLE IF NOT EXISTS clicks (
            produkt_id BIGINT,
            haendler_bez STRING,
            timestamp BIGINT
        )
    """)
