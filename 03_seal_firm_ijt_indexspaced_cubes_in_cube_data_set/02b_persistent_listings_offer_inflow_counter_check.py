import csv
import logging

import polars as pl
from tqdm import tqdm

from impl.db.datasource import DuckDBDataSource
from impl.db.loaders.init_db import DatabaseInitializer
from impl.helpers import date_to_unix_time
from impl.repository.seal_change_firms_repository import SealChangeFirmsDataRepository

# Constants
DUCKDB_THREADS = 512
WINDOW_SIZE_WEEKS = 26
INFLOW_WINDOW_PRE_WEEKS = WINDOW_SIZE_WEEKS * 2

OBSERVATION_WINDOW_SEC_LOWER_BOUND_INTERCEPT = INFLOW_WINDOW_PRE_WEEKS * 7 * 24 * 3600
OBSERVATION_WINDOW_SEC_UPPER_BOUND_INTERCEPT = WINDOW_SIZE_WEEKS * 7 * 24 * 3600

OFFER_PARQUET_FILES_WILDCARDED_PATH = '/scratch0/zieg/MeJ_Tests.d/angebot_*_*.pq/angebot_20*w*.parquet'

OUTPUT_CSV = 'affected.csv'


# Logging setup
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='02b_persistent_listings_offer_inflow_counter_check.log',
                    filemode='w')
logger = logging.getLogger(__name__)


def fetch_seal_change_firms(db) -> pl.DataFrame:
    sealChangeFirmsRepo = SealChangeFirmsDataRepository(db)
    return sealChangeFirmsRepo.fetch_all()


def generate_and_execute_queries(db: DuckDBDataSource, retailer_name, seal_date_timestamp):
    query = f"""
    SELECT produkt_id, haendler_bez, dtimebegin, dtimeend
    FROM read_parquet(['{OFFER_PARQUET_FILES_WILDCARDED_PATH}'])
        WHERE haendler_bez = '{retailer_name}'
        AND dtimebegin < ({seal_date_timestamp} - {OBSERVATION_WINDOW_SEC_LOWER_BOUND_INTERCEPT})
        AND dtimeend > ({seal_date_timestamp} + {OBSERVATION_WINDOW_SEC_UPPER_BOUND_INTERCEPT});
    """

    logger.info(f"Executing query for retailer: {retailer_name}")
    logger.debug(f"Generated query:\n{query}")

    result = db.queryAsPl(query)

    if result.is_empty():
        logger.info(f"Retailer {retailer_name}: No affected products found.")
        return 0, 0

    logger.info(result)

    unique_products = result['produkt_id'].unique().len()

    num_rows = result.height

    logger.info(f"Retailer {retailer_name}: {unique_products} unique products, {num_rows} rows found.")

    return unique_products, num_rows


def calculate_persistent_listing_criterion_affected(database: DuckDBDataSource):
    seal_change_firms = fetch_seal_change_firms(database)

    with open(OUTPUT_CSV, mode='w', newline='') as csvfile:

        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['haendler_bez', 'affected_products', 'num_rows'])

        seq = range(0, len(seal_change_firms))

        for index, row in tqdm(enumerate(seq),
                               total=len(seal_change_firms),
                               desc="Processing retailers"):

            retailer_name = seal_change_firms[index, 2]
            seal_date_timestamp = date_to_unix_time(str(seal_change_firms[index, 3]),
                                                    '%Y-%m-%d')

            logger.info(f"Processing retailer {retailer_name} with seal date UNIX {seal_date_timestamp}")

            affected_products, num_rows = generate_and_execute_queries(database,
                                                                       retailer_name,
                                                                       seal_date_timestamp)

            csvwriter.writerow([retailer_name, affected_products, num_rows])


if __name__ == '__main__':
    logger.info("Initializing in-memory database.")

    database = DuckDBDataSource(':memory:',
                                threads=DUCKDB_THREADS,
                                bypass_application_thread_config=True)

    db_initializer = DatabaseInitializer(database)
    db_initializer.initialize_database()
    logger.info("DB initialized.")

    calculate_persistent_listing_criterion_affected(database)
    logger.info(f"Results written to {OUTPUT_CSV}.")
