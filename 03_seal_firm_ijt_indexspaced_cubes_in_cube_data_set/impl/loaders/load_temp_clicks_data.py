from tqdm import tqdm
import os
from CONFIG import CLICKS_SCHEME, PARQUET_FILES_DIR, CLICKS_FOLDER
from impl.main import get_year_month_from_seal_date, generate_months_around_seal
from impl.db.querybuilder import QueryBuilder
from impl.db.datasource import DuckDBDataSource
import logging

logger = logging.getLogger(__name__)


def load_click_data(
        db: DuckDBDataSource,
        seal_date,
        columns=None,
        table_name='clicks_temp',
        parquet_dir=PARQUET_FILES_DIR,
        file_scheme=CLICKS_SCHEME
):
    if columns is None:
        columns = ['produkt_id', 'haendler_bez', 'timestamp']

    seal_year, seal_month = get_year_month_from_seal_date(seal_date)
    relevant_months = generate_months_around_seal(seal_year, seal_month)
    total_files = len(relevant_months)
    table_created = False

    with tqdm(total=total_files, desc="Loading Click Data", unit="file", ncols=100) as pbar:
        for month in relevant_months:
            file_name = file_scheme.format(year=seal_year, month=f"{month:02d}")
            file_path = parquet_dir / file_name

            if os.path.isfile(file_path):
                if not table_created:
                    logger.info(f"Creating table from {file_path}")
                    db.load_parquet_to_table(file_path, table_name, columns=columns)
                    table_created = True
                else:
                    logger.info(f"Appending data from {file_path}")
                    db.append_parquet_to_table(file_path, table_name, columns=columns)

            pbar.update(1)

    if table_created:
        count_query = QueryBuilder(table_name) \
            .select('COUNT(*) AS count') \
            .build()

        count_result = db.query(count_query)
        count = count_result.iloc[0]['count']
        logger.info(f"Total Click data rows loaded: {count}")
        return count
    else:
        logger.warning(f"No relevant Click data found for seal date {seal_date}.")
        return 0


def load_click_data_v2(
        db: DuckDBDataSource,
        seal_date,
        table_name='clicks_temp',
        parquet_dir=PARQUET_FILES_DIR,
        file_scheme=CLICKS_SCHEME,
        click_folder=CLICKS_FOLDER
):
    seal_year, seal_month = get_year_month_from_seal_date(seal_date)
    relevant_months = generate_months_around_seal(seal_year, seal_month)

    for month in relevant_months:
        file_name = file_scheme.format(year=seal_year, month=f"{month:02d}")
        file_path = parquet_dir / click_folder / file_name

        if os.path.isfile(file_path):
            logger.info(f"Loading Clicks data from {file_path}")
            db.load_parquet_to_table(file_path, table_name)


def initialize_clicks_table(db: DuckDBDataSource, table_name='clicks'):
    db.query(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            produkt_id BIGINT,
            haendler_bez STRING,
            timestamp BIGINT
        )
    """)