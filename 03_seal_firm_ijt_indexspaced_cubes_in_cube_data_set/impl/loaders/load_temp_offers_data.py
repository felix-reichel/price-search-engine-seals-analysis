from tqdm import tqdm
import os

import CONFIG
from CONFIG import ANGEBOTE_SCHEME, PARQUET_FILES_DIR, ANGEBOTE_FOLDER
from impl.helpers import get_week_year_from_seal_date, generate_weeks_around_seal
from impl.db.querybuilder import QueryBuilder
from impl.db.datasource import DuckDBDataSource
import logging

logger = logging.getLogger(__name__)


def load_angebot_data(db: DuckDBDataSource,
                      seal_date,
                      columns=None,
                      pre_seal_weeks=None,
                      post_seal_weeks=None):
    if columns is None:
        columns=['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend']

    seal_year, seal_week = get_week_year_from_seal_date(seal_date)

    if pre_seal_weeks is None:
        pre_seal_weeks = CONFIG.OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED

    if post_seal_weeks is None:
        post_seal_weeks = CONFIG.OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED

    relevant_weeks = generate_weeks_around_seal(
        seal_year,
        seal_week,
        pre_seal_weeks,
        post_seal_weeks
    )

    total_files = len(relevant_weeks)
    table_initialized = False

    with tqdm(total=total_files, desc="Loading Offer Data", unit="file", ncols=100) as progress_bar:
        for week in relevant_weeks:
            file_name = ANGEBOTE_SCHEME.format(year=seal_year, week=week)
            file_path = PARQUET_FILES_DIR / file_name

            if os.path.isfile(file_path):
                if not table_initialized:
                    logger.info(f"Initializing table from {file_path}")
                    db.load_parquet_to_table(file_path, 'offer_temp', columns=columns)
                    table_initialized = True
                else:
                    logger.info(f"Adding data from {file_path}")
                    db.append_parquet_to_table(file_path, 'offer_temp', columns=columns)

            progress_bar.update(1)

    if table_initialized:
        count_query = (
            QueryBuilder('offer_temp')
            .select('COUNT(*) AS total_rows')
            .where("haendler_bez IN (SELECT haendler_bez FROM filtered_haendler_bez)")
            .build()
        )

        result = db.query(count_query)
        total_rows = result.iloc[0]['total_rows']
        logger.info(f"Total rows in Offer data: {total_rows}")
        return total_rows
    else:
        logger.warning(f"No relevant offer data found for seal date {seal_date}.")
        return 0


def load_angebot_data_v2(db: DuckDBDataSource,
                         seal_date_str,
                         offer_folder=None,
                         table_name='offer_temp'):
    seal_year, seal_week = get_week_year_from_seal_date(seal_date_str)
    relevant_weeks = generate_weeks_around_seal(seal_year, seal_week,
                                                CONFIG.OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED,
                                                CONFIG.OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED)

    for week in relevant_weeks:
        file_name = ANGEBOTE_SCHEME.format(year=seal_year, week=week)

        if offer_folder is None:
            if seal_year <= 2010:
                offer_folder = CONFIG.ANGEBOTE_FOLDER_1
            if 2010 < seal_year <= 2015:
                offer_folder = CONFIG.ANGEBOTE_FOLDER_2
            elif seal_year > 2015:
                offer_folder = CONFIG.ANGEBOTE_FOLDER_3

        file_path = str(PARQUET_FILES_DIR) + offer_folder + file_name

        if os.path.isfile(file_path):
            logger.info(f"Loading offer data from {file_path}")
            db.load_parquet_to_table(file_path, table_name)

            insert_query = (
                QueryBuilder(table_name)
                .select('*')
                .where("haendler_bez IN (SELECT haendler_bez FROM filtered_haendler_bez)")
                .insert_into('offer')
            )
            db.query(insert_query)


def initialize_offer_table(db: DuckDBDataSource,
                           table_name='angebot'):
    db.query(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            produkt_id BIGINT,
            haendler_bez STRING,
            dtimebegin BIGINT,
            dtimeend BIGINT
        )
    """)