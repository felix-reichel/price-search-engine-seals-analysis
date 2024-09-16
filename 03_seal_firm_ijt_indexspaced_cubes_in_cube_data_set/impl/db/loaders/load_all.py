from impl.db.duckdb_data_source import DuckDBDataSource
import polars as pl
import logging
from tqdm import tqdm
import glob
from CONFIG import FILTERED_HAENDLER_BEZ

logger = logging.getLogger(__name__)

# angebot_parquet_patterns = [
#     '/scratch0/zieg/MeJ_Tests.d/angebot_06_10.pq/*.parquet',
#     '/scratch0/zieg/MeJ_Tests.d/angebot_11_14.pq/*.parquet',
#    '/scratch0/zieg/MeJ_Tests.d/angebot_15_19.pq/*.parquet',
#    '/scratch0/zieg/MeJ_Tests.d/angebot_20_24.pq/*.parquet'
# ]
angebot_parquet_patterns = [
    '../../../data/angebot/*.parquet',
]


# clicks_parquet_pattern = '/scratch0/zieg/MeJ_Tests.d/clicks.pq/*.parquet'
clicks_parquet_pattern = [
    '../data/clicks/*.parquet',
]


def load_all_data(allowed_firms):
    # TODO:
    # db = DuckDBDataSource('/scratch0/zieg/MeJ_Tests.d/MeJ_Tests.db')
    # db = DuckDBDataSource(':memory:')
    db = DuckDBDataSource('localduck.db')


    def prepare_angebot_table():
        db.query("""
            CREATE TABLE IF NOT EXISTS angebot (
                produkt_id BIGINT,
                haendler_bez STRING,
                dtimebegin BIGINT,
                dtimeend BIGINT
            );
        """)

    def prepare_clicks_table():
        db.query("""
            CREATE TABLE IF NOT EXISTS clicks (
                produkt_id BIGINT,
                haendler_bez STRING,
                click_timestamp BIGINT
            );
        """)

    prepare_angebot_table()
    prepare_clicks_table()

    def load_relevant_angebot_data():
        angebot_parquet_files = []
        for pattern in angebot_parquet_patterns:
            angebot_parquet_files.extend(glob.glob(pattern))

        total_files = len(angebot_parquet_files)
        with tqdm(total=total_files, desc="Loading Angebot Data", unit="file", ncols=100) as pbar:
            for file_path in angebot_parquet_files:
                logger.info(f"Loading Angebot data from {file_path}")

                db.load_parquet_to_table(
                    file_path,
                    'angebot_temp',
                     columns=['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend'])


                # WHERE haendler_bez IN {tuple(allowed_firms)};
                db.query(f"""
                    INSERT INTO angebot SELECT * FROM angebot_temp; 
                """)
                pbar.update(1)

    def load_relevant_clicks_data():
        clicks_parquet_files = glob.glob(clicks_parquet_pattern)
        for file_path in clicks_parquet_files:
            logger.info(f"Loading Clicks data from {file_path}")
            db.load_parquet_to_table(
                file_path,
                'clicks_temp',
                columns=['produkt_id', 'haendler_bez', 'timestamp'])

            db.query(f"""
                INSERT INTO clicks SELECT * FROM clicks_temp
                WHERE haendler_bez IN {tuple(allowed_firms)};
            """)

    load_relevant_angebot_data()
    # load_relevant_clicks_data()

    print("Verifying Angebot table record count.")
    result_angebot = db.query("SELECT COUNT(*) AS count FROM angebot;")
    print(f"Angebot table record count: {result_angebot.iloc[0]['count']}")

    print("Verifying Clicks table record count.")
    result_clicks = db.query("SELECT COUNT(*) AS count FROM clicks;")
    print(f"Clicks table record count: {result_clicks.iloc[0]['count']}")

    db.close()


if __name__ == "__main__":
    # TODO:
    allowed_firms = pl.read_csv('../../' + FILTERED_HAENDLER_BEZ)[';0'].to_list()
    load_all_data(allowed_firms)
