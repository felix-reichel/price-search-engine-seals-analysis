import logging
import os

from tqdm import tqdm

from CONFIG import CLICKS_SCHEME, PARQUET_FILES_DIR, CLICKS_FOLDER
from impl.db.datasource import DuckDBDataSource
from impl.db.simple_sql_base_query_builder import SimpleSQLBaseQueryBuilder
from impl.helpers import get_year_month_from_seal_date, generate_months_around_seal, file_exists_in_folders

logger = logging.getLogger(__name__)


def load_selection_criteria_inflow_click_data(
        db: DuckDBDataSource,
        seal_date,
        columns=None,
        table_name='clicks',
        parquet_dir=PARQUET_FILES_DIR,
        file_scheme=CLICKS_SCHEME
):
    """
    Load click data from Parquet files around a specific seal date.

    Parameters:
    db (DuckDBDataSource): The database connection instance.
    seal_date (any): The seal date used to determine which files to load.
    columns (list, optional): The columns to load from Parquet. Defaults to ['produkt_id', 'haendler_bez', 'timestamp'].
    table_name (str, optional): The name of the table to load the data into. Defaults to 'clicks'.
    parquet_dir (Path, optional): Directory containing the Parquet files. Defaults to PARQUET_FILES_DIR.
    file_scheme (str, optional): File naming scheme for Parquet files. Defaults to CLICKS_SCHEME.
    """
    if columns is None:
        columns = ['produkt_id', 'haendler_bez', 'timestamp']

    seal_year, seal_month = get_year_month_from_seal_date(seal_date)
    relevant_months = generate_months_around_seal(seal_year, seal_month)
    total_files = len(relevant_months)
    table_created = False

    with tqdm(total=total_files, desc="Loading Click Data", unit="file", ncols=100) as pbar:
        for month in relevant_months:
            file_name = month
            file_path = file_exists_in_folders(file_name, CLICKS_FOLDER)

            if file_path:
                if not table_created:
                    logger.info(f"Creating table from {file_path}")
                    db.load_parquet_to_table(file_path, table_name, columns=columns)
                    table_created = True
                else:
                    logger.info(f"Appending data from {file_path}")
                    db.append_parquet_to_table(file_path, table_name, columns=columns)

            pbar.update(1)

    if table_created:

        # POST INIT
        count_query = (
            SimpleSQLBaseQueryBuilder('clicks')
            .select(
                'COUNT(*) AS total_loaded_inflow_rows')
            # .where("haendler_bez IN (SELECT haendler_bez FROM filtered_haendler_bez)")
            .build()
        )
        result = db.queryAsPl(count_query)
        logger.info(f"Total rows in Clicks data: {result[0][0]}")

        # DB BULK DELETE (WORK-AROUND) Todo: may check later
        # db.conn.execute("""
        #    DELETE FROM clicks cl
        #    WHERE NOT EXISTS ( SELECT 42 FROM filtered_haendler_bez WHERE haendler_bez = cl.haendler_bez )
        # """)
        #
        # db.conn.execute("""
        #     UPDATE clicks cl
        #     SET haendler_bez = NULL
        #     WHERE NOT EXISTS ( SELECT 42 FROM filtered_haendler_bez WHERE haendler_bez = cl.haendler_bez )
        # """)
        #
        # db.conn.execute("""
        #     DELETE FROM clicks
        #     WHERE haendler_bez IS NULL
        # """)
        #
        # # POST DB BULK DELETE
        # result = db.queryAsPl(count_query)
        # logger.info(f"*NEW Total rows in Clicks data: {result[0][0]}")

        return result[0][0]
    else:
        logger.warning(f"No relevant Clicks data found for seal date {seal_date}.")
        return 0


@PendingDeprecationWarning
def load_click_data_v2(
        db: DuckDBDataSource,
        seal_date,
        table_name='clicks',
        parquet_dir=PARQUET_FILES_DIR,
        file_scheme=CLICKS_SCHEME,
        click_folder=CLICKS_FOLDER
):
    """
    Load click data using an alternative approach based on file availability.

    Parameters:
    db (DuckDBDataSource): The database connection instance.
    seal_date (any): The seal date used to determine which files to load.
    table_name (str, optional): The name of the table to load the data into. Defaults to 'clicks'.
    parquet_dir (Path, optional): Directory containing the Parquet files. Defaults to PARQUET_FILES_DIR.
    file_scheme (str, optional): File naming scheme for Parquet files. Defaults to CLICKS_SCHEME.
    click_folder (str, optional): The folder where click files are stored. Defaults to CLICKS_FOLDER.
    """
    seal_year, seal_month = get_year_month_from_seal_date(seal_date)
    relevant_months = generate_months_around_seal(seal_year, seal_month)

    for month in relevant_months:
        file_name = file_scheme.format(year=seal_year, month=f"{month:02d}")
        file_path = parquet_dir / click_folder / file_name

        if os.path.isfile(file_path):
            logger.info(f"Loading Clicks data from {file_path}")
            db.gz_load_filtered_parquet_to_table(file_path, table_name)


def initialize_clicks_table(
        db: DuckDBDataSource,
        table_name='clicks'
):
    """
    Initialize the clicks table with the necessary schema.

    Parameters:
    db (DuckDBDataSource): The database connection instance.
    table_name (str, optional): The name of the clicks table to initialize. Defaults to 'clicks'.
    """
    db.queryAsPl(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            produkt_id BIGINT,
            haendler_bez STRING,
            timestamp BIGINT
        )
    """)
