import logging

from impl.db.datasource import DuckDBDataSource
from impl.db.loaders.init_db import DatabaseInitializer
from impl.factory import Factory

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='01_init_global_seal_analysis_params.log', filemode='w')
logger = logging.getLogger(__name__)


if __name__ == '__main__':

    # Step 0 - init DB
    db = DuckDBDataSource()
    db_initializer = DatabaseInitializer(db)
    db_initializer.initialize_database()

    logger.info("Verifying that required tables are present.")
    required_tables = ['seal_change_firms', 'filtered_haendler_bez', 'products', 'retailers']

    for table in required_tables:
        result = db.queryAsPl(f"SELECT COUNT(*) FROM {table}")
        logger.info(f"Table {table} exists with {result[0][0]} rows.")

    # Step 1 - init global seal params
    # Using repositories directly, as they don't have corresponding services

    filtered_retailer_names_repo = Factory.create_filtered_retailer_names_repository()
    seal_change_firms_repo = Factory.create_seal_change_firms_repository()

    allowed_firms = filtered_retailer_names_repo.fetch_all_filtered_retailers()
    seal_change_firms = seal_change_firms_repo.fetch_all()

    seal_firms = seal_change_firms
    print(seal_firms)
