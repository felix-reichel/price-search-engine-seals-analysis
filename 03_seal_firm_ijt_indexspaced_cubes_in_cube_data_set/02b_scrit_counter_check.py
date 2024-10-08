import logging
import sys
from tqdm import tqdm
from impl.db.datasource import DuckDBDataSource
from impl.db.loaders.init_db import DatabaseInitializer

DUCKDB_THREADS = 32
WINDOW_SIZE_WEEKS = 26
OBSERVATION_WINDOW_SEC = WINDOW_SIZE_WEEKS * 7 * 24 * 3600

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger(__name__)


def fetch_seal_change_firms(db):
    query = """
    SELECT matched_haendler_bez AS haendler_bez, UNIX_TIMESTAMP(CAST(seal_date_str AS TIMESTAMP)) AS seal_date_ts
    FROM seal_change_firms
    """
    logger.info("Fetching seal change firms data.")
    data = db.queryAsPl(query).to_pandas()
    logger.info(f"Fetched {len(data)} seal change firms.")
    return data


def generate_and_execute_queries(duckdb_conn, retailer_name, seal_date_timestamp):
    query = f"""
    SELECT produkt_id, haendler_bez, COUNT(*) AS num_records,
           MIN(dtimebegin) AS first_listed, MAX(dtimeend) AS last_listed
    FROM read_parquet([
        '/scratch0/zieg/MeJ_Tests.d/angebot_06_10.pq/angebot_20??w*.parquet',
        '/scratch0/zieg/MeJ_Tests.d/angebot_11_14.pq/angebot_20??w*.parquet',
        '/scratch0/zieg/MeJ_Tests.d/angebot_15_19.pq/angebot_20??w*.parquet',
        '/scratch0/zieg/MeJ_Tests.d/angebot_20_24.pq/angebot_20??w*.parquet'
    ], union_by_name=True)
    WHERE haendler_bez = '{retailer_name}'
    GROUP BY produkt_id, haendler_bez
    HAVING MIN(dtimebegin) < ({seal_date_timestamp} - {OBSERVATION_WINDOW_SEC})
       AND MAX(dtimeend) > ({seal_date_timestamp} + {OBSERVATION_WINDOW_SEC})
       AND COUNT(*) >= 1;
    """

    logger.info(f"Executing query for retailer: {retailer_name}")
    logger.debug(f"Generated query:\n{query}")

    result = duckdb_conn.execute(query).fetchdf()

    if result.empty:
        logger.info(f"Retailer {retailer_name}: No unchanged products found.")
    else:
        logger.info(f"Query res. for ret. {retailer_name}:\n{result}")


def main(database: DuckDBDataSource):
    seal_change_firms = fetch_seal_change_firms(database)
    logger.info(f"Total seal change firms fetched: {len(seal_change_firms)}")

    for index, row in tqdm(seal_change_firms.iterrows(),
                           total=len(seal_change_firms),
                           desc="Processing retailers"):
        retailer_name = row['haendler_bez']
        seal_date_timestamp = row['seal_date_ts']
        logger.info(f"Processing retailer {retailer_name} with seal {seal_date_timestamp}")
        generate_and_execute_queries(database.conn, retailer_name, seal_date_timestamp)


if __name__ == '__main__':
    logger.info("Initializing in-memory database.")
    database = DuckDBDataSource(':memory:', threads=DUCKDB_THREADS)
    db_initializer = DatabaseInitializer(database)
    db_initializer.initialize_database()
    logger.info("DB initialized.")
    main(database)
