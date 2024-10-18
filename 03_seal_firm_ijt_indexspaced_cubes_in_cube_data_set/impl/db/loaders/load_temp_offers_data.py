import logging
import os

from tqdm import tqdm

import CONFIG
from CONFIG import ANGEBOTE_FOLDER
from impl.db.datasource import DuckDBDataSource
from impl.db.simple_sql_base_query_builder import SimpleSQLBaseQueryBuilder
from impl.helpers import get_week_year_from_seal_date, generate_weeks_around_seal, file_exists_in_folders

logger = logging.getLogger(__name__)


def load_angebot_data(
    db: DuckDBDataSource,
    seal_date,
    columns=None,
    pre_seal_weeks=None,
    post_seal_weeks=None
):
    """
    Load offer data from Parquet files around a specific seal date.

    Parameters:
    db (DuckDBDataSource): The database connection instance.
    seal_date (any): The seal date used to determine which files to load.
    columns (list, optional): The columns to load from Parquet. Defaults to ['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend'].
    pre_seal_weeks (int, optional): Number of weeks before the seal date to consider. Defaults to CONFIG.OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED.
    post_seal_weeks (int, optional): Number of weeks after the seal date to consider. Defaults to CONFIG.OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED.
    """
    if columns is None:
        columns = ['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend']

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
            file_name = week
            file_path = file_exists_in_folders(file_name, ANGEBOTE_FOLDER)

            if file_path:
                if not table_initialized:
                    logger.info(f"Initializing table from {file_path}")
                    db.load_parquet_to_table(file_path, 'angebot', columns=columns)
                    table_initialized = True
                else:
                    logger.info(f"Adding data from {file_path}")
                    db.append_parquet_to_table(file_path, 'angebot', columns=columns)

            progress_bar.update(1)

    if table_initialized:
        count_query = (
            SimpleSQLBaseQueryBuilder('angebot')
            .select('COUNT(*) AS total_loaded_inflow_rows')    # .where("haendler_bez IN (SELECT haendler_bez FROM filtered_haendler_bez)")
            .build()
        )

        result = db.queryAsPl(count_query)
        logger.info(f"Total rows in Offer data: {result[0][0]}")
        return result[0][0]
    else:
        logger.warning(f"No relevant offer data found for seal date {seal_date}.")
        return 0


@PendingDeprecationWarning
def load_angebot_data_v2(
    db: DuckDBDataSource,
    seal_date_str,
    offer_folder=None,
    table_name='angebot'
):
    """
    Alternative method to load offer data from Parquet files.

    Parameters:
    db (DuckDBDataSource): The database connection instance.
    seal_date_str (str): The seal date as a string to determine which files to load.
    offer_folder (str, optional): The folder containing offer files. If None, folder is selected based on the seal year.
    table_name (str, optional): The name of the table to load data into. Defaults to 'angebot'.
    """
    seal_year, seal_week = get_week_year_from_seal_date(seal_date_str)
    relevant_weeks = generate_weeks_around_seal(
        seal_year,
        seal_week,
        CONFIG.OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED,
        CONFIG.OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED
    )

    for week in relevant_weeks:
        file_name = CONFIG.ANGEBOTE_SCHEME.format(year=seal_year, week=week)

        if offer_folder is None:
            if seal_year <= 2010:
                offer_folder = CONFIG.ANGEBOTE_FOLDER_1
            elif 2010 < seal_year <= 2015:
                offer_folder = CONFIG.ANGEBOTE_FOLDER_2
            elif seal_year > 2015:
                offer_folder = CONFIG.ANGEBOTE_FOLDER_3

        file_path = str(CONFIG.PARQUET_FILES_DIR) + offer_folder + file_name

        if os.path.isfile(file_path):
            logger.info(f"Loading offer data from {file_path}")
            db.load_parquet_to_table(file_path, table_name)

            insert_query = (
                SimpleSQLBaseQueryBuilder(table_name)
                .select('*')
                .where("haendler_bez IN (SELECT haendler_bez FROM filtered_haendler_bez)")
                .insert_into('angebot')
            )
            db.queryAsPl(insert_query)


def initialize_offer_table(
    db: DuckDBDataSource,
    table_name='angebot'
):
    """
    Initialize the 'angebot' table with the necessary schema.

    Parameters:
    db (DuckDBDataSource): The database connection instance.
    table_name (str, optional): The name of the 'angebot' table. Defaults to 'angebot'.
    """
    db.queryAsPl(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            produkt_id BIGINT,
            haendler_bez STRING,
            dtimebegin BIGINT,
            dtimeend BIGINT
        )
    """)
