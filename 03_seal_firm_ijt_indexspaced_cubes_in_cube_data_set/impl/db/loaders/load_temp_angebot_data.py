from tqdm import tqdm
import os
from CONFIG import ANGEBOTE_SCHEME, PARQUET_FILES_DIR, ANGEBOTE_FOLDER
from impl.main import get_week_year_from_seal_date, generate_weeks_around_seal
from impl.db.query_builder import QueryBuilder
from impl.db.duckdb_data_source import DuckDBDataSource
import logging

logger = logging.getLogger(__name__)


def load_relevant_angebot_data(db: DuckDBDataSource,
                               seal_date,
                               allowed_firms,
                               columns=None):

    if columns is None:
        columns = ['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend']

    seal_year, seal_week = get_week_year_from_seal_date(seal_date)
    relevant_weeks = generate_weeks_around_seal(seal_year, seal_week)

    total_files = len(relevant_weeks)
    table_created = False

    with tqdm(total=total_files, desc="Loading Angebot Data", unit="file", ncols=100) as pbar:
        for week in relevant_weeks:
            file_name = ANGEBOTE_SCHEME.format(year=seal_year, week=f"{week:02d}")
            file_path = PARQUET_FILES_DIR / file_name

            if os.path.isfile(file_path):
                if not table_created:
                    logger.info(f"Creating table from {file_path}")
                    db.load_parquet_to_table(file_path, 'angebot_temp', columns=columns)
                    table_created = True
                else:
                    logger.info(f"Appending data from {file_path}")
                    db.append_parquet_to_table(file_path, 'angebot_temp', columns=columns)

            pbar.update(1)

    if table_created:
        count_query = QueryBuilder('angebot_temp') \
            .select('COUNT(*) AS total_rows') \
            .where(f"haendler_bez IN {tuple(allowed_firms)}") \
            .build()

        result = db.query(count_query)
        total_rows = result.iloc[0]['total_rows']

        logger.info(f"Total rows in Angebot data: {total_rows}")
        return total_rows

    else:
        logger.warning(f"No relevant Angebot data found for seal date {seal_date}.")
        return 0



def load_relevant_angebot_data_v2(db, seal_date, allowed_firms):
    """
    Load relevant Angebot data around the seal date into DuckDB.
    """
    seal_year, seal_week = get_week_year_from_seal_date(seal_date)
    relevant_weeks = generate_weeks_around_seal(seal_year, seal_week)

    for week in relevant_weeks:
        file_name = ANGEBOTE_SCHEME.format(year=seal_year, week=f"{week:02d}")
        file_path = PARQUET_FILES_DIR / ANGEBOTE_FOLDER / file_name

        if os.path.isfile(file_path):
            logger.info(f"Loading Angebot data from {file_path}")
            db.load_parquet_to_table(file_path, 'angebot_temp')

            query = f"""
                INSERT INTO angebot
                SELECT * FROM angebot_temp WHERE haendler_bez IN {tuple(allowed_firms)}
            """
            db.query(query)


def prepare_angebot_table(db):
    db.query("""
        CREATE TABLE IF NOT EXISTS angebot (
            produkt_id BIGINT,
            haendler_bez STRING,
            dtimebegin BIGINT,
            dtimeend BIGINT
        )
    """)
