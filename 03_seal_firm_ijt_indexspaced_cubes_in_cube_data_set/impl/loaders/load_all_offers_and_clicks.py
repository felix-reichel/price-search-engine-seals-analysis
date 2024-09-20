import glob
import logging
from tqdm import tqdm
from impl.db.datasource import DuckDBDataSource
from impl.db.querybuilder import QueryBuilder

logger = logging.getLogger(__name__)

angebot_file_patterns = [
    '/scratch0/zieg/MeJ_Tests.d/angebot_06_10.pq/*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_11_14.pq/*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_15_19.pq/*.parquet',
    '/scratch0/zieg/MeJ_Tests.d/angebot_20_24.pq/*.parquet'
]

click_file_pattern = [
    '/scratch0/zieg/MeJ_Tests.d/clicks.pq/*.parquet'
]


def initialize_tables(
        db: DuckDBDataSource
):
    db.query(
        """
        CREATE TABLE IF NOT EXISTS angebot (
            produkt_id BIGINT,
            haendler_bez STRING,
            dtimebegin BIGINT,
            dtimeend BIGINT
        );
        """
    )
    db.query(
        """
        CREATE TABLE IF NOT EXISTS clicks (
            produkt_id BIGINT,
            haendler_bez STRING,
            click_timestamp BIGINT
        );
        """
    )


def import_angebot_data(
        db: DuckDBDataSource
):
    angebot_files = [
        file
        for pattern in angebot_file_patterns
        for file in glob.glob(pattern)
    ]
    total_files = len(angebot_files)

    with tqdm(
            total=total_files,
            desc="Loading Angebot Data",
            unit="file",
            ncols=100
    ) as progress_bar:
        for file_path in angebot_files:
            logger.info(f"Loading Angebot data from {file_path}")

            db.load_parquet_to_table(
                file_path,
                'angebot_temp',
                columns=['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend']
            )

            insert_query = (
                QueryBuilder('angebot_temp')
                .select(['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend'])
                .where(
                    f"haendler_bez IN (SELECT haendler_bez FROM filtered_haendler_bez)"
                )
                .insert_into('angebot')
            )
            db.query(insert_query)
            progress_bar.update(1)


def import_click_data(
        db: DuckDBDataSource
):
    click_files = glob.glob(click_file_pattern[0])

    for file_path in click_files:
        logger.info(f"Loading Click data from {file_path}")

        db.load_parquet_to_table(
            file_path,
            'clicks_temp',
            columns=['produkt_id', 'haendler_bez', 'timestamp']
        )

        insert_query = (
            QueryBuilder('clicks_temp')
            .select(['produkt_id', 'haendler_bez', 'timestamp'])
            .where(
                f"haendler_bez IN (SELECT haendler_bez FROM filtered_haendler_bez)"
            )
            .insert_into('clicks')
        )
        db.query(insert_query)


def load_data(
        db: DuckDBDataSource
):
    initialize_tables(db)

    import_angebot_data(db)
    import_click_data(db)

    check_table_record_count(db, "Angebot", "angebot")
    check_table_record_count(db, "Click", "clicks")


def check_table_record_count(
        db: DuckDBDataSource,
        table_label: str,
        table_name: str
):
    logger.info(f"Checking {table_label} table record count.")
    count_query = QueryBuilder(table_name).select("COUNT(*) AS count").build()
    result = db.query(count_query)
    logger.info(f"{table_label} table record count: {result.iloc[0]['count']}")


def run(
        db: DuckDBDataSource
):
    load_data(db)
