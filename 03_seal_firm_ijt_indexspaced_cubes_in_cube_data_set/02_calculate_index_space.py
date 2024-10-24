import os
import logging
import threading
import time
import psutil
from concurrent.futures import ThreadPoolExecutor, as_completed
from multiprocessing import Manager, Value
from functools import lru_cache

import polars as pl
from tqdm import tqdm

import CONFIG
from impl.db.datasource import DuckDBDataSource
from impl.db.loaders.init_db import DatabaseInitializer
from impl.db.loaders.load_temp_clicks_data import initialize_clicks_table, load_selection_criteria_inflow_click_data
from impl.db.loaders.load_temp_offers_data import initialize_offer_table, load_selection_criteria_inflow_angebot_data
from impl.factory import Factory
from impl.helpers import calculate_running_var_t_from_u, print_process_mem_usage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='02_calculate_index_space.log', filemode='w')
logger = logging.getLogger(__name__)

# boundedSemaphore = threading.BoundedSemaphore(120)


def monitor_prcss_threads():
    process = psutil.Process()
    while True:
        thread_count = process.num_threads()
        logger.info(f"Current number of threads: {thread_count}")
        print(f"Current number of threads: {thread_count}")
        time.sleep(20)


def free_up_memory_and_drop_table(db: DuckDBDataSource, table_name: str):
    """Free memory by dropping a table and displaying memory usage."""
    print_process_mem_usage()
    db.free_up_table_and_manipulate_file_logs(table_name)
    logger.info(f"Table '{table_name}' dropped to free memory.")
    time.sleep(60)  # Allow time for resources to be freed.
    print_process_mem_usage()


@lru_cache(maxsize=None)
def get_cached_running_var(unix_time):
    """Cached version of calculate_running_var_t_from_u."""
    return calculate_running_var_t_from_u(unix_time)


def process_single_product(product, firm_id, seal_date_str, product_service, has_seal):
    """Process a single product and return relevant data."""
    offered_weeks = product_service.get_offered_weeks(product, firm_id, seal_date_str)
    # TODO:
    # TYPE 1 Variables - inferred during the selection criteria process.
    ################################################
    # Own helpers (positional info)
    # 'firm_has_seal_j' == 1 = indicates the MAIN seal change firm
    # 'seal_change_cluster_zugehoerigkeit_categorical': 0 .. 296
    # ...
    # Other helpers:
    # ...
    # Specs:
    # siegel_anbieter1_jt  # binary dummy
    # siegel_anbieter2_jt  # binary dummy
    # siegel_anbieter3_jt  # binary dummy
    # binary seal dummies are 0 for counterfactual firms J not element of J_seal

    # ...

    # Type 1 Variables do not use UniqueTupleExtractor or additional Loader Classes.
    ################################################

    # TYPE 2 Variables using routine of https://github.com/felix-reichel/price-search-engine-seals-analysis/issues/29
    # stored to in Step 1 Variable produced Grid / DataFrame / "Results" -Table.
    # deps.: uses UniqueTupleExtractor (for POSITIONAL (Where) UPDATE-Queries affecting multiple Cells / Variables /
    # Features / Entries)
    ################################################

    # TYPE 3 Variables (Augmented Data) - Exogenous Instrumental Variables (IVs)
    # Can be produced solely with Tuples from UniqueTupleExtractor and PQ data base and some repos / services.
    # IV_ijt
    # deps.:
    # uses UniqueTupleExtractor
    ################################################
    return [{'produkt_id': product, 'haendler_bez': firm_id, 'week_running_var': week, 'firm_has_seal_j': has_seal}
            for week in offered_weeks]


def gather_tasks(product, seal_date_str, seal_firms: pl.Series, geizhals_id, allowed_firms: pl.Series, product_service):
    """Gather all tasks related to a product and its counterfactual firms."""
    main_task = [(product, geizhals_id, seal_date_str, True, product_service)]
    counterfactual_firms = product_service.get_rand_max_N_counterfactual_firms(product, seal_date_str, seal_firms,
                                                                               allowed_firms)
    counterfactual_tasks = [(product, firm, seal_date_str, False, product_service) for firm in counterfactual_firms]
    logger.info(f"Total tasks for product {product}: {len(main_task + counterfactual_tasks)}")
    return main_task + counterfactual_tasks


def process_task(args):
    """Process a single task and return the result."""
    #  with boundedSemaphore:
    product, firm, seal_date_str, is_main_firm, product_service = args
    return process_single_product(product, firm, seal_date_str, product_service, int(is_main_firm))


def process_seal_firm(seal_firm_data, result_counter, db: DuckDBDataSource):
    """Process all tasks related to a single seal firm."""
    haendler_bez, geizhals_id, seal_date, seal_firms, allowed_firms, processed_firms, product_service, clicks_service = seal_firm_data
    firm_seal_key = (haendler_bez, seal_date)

    # Check if the firm_seal_key has already been processed
    if firm_seal_key in processed_firms:
        logger.info(f"Firm {haendler_bez} with seal date {seal_date} already processed. Skipping.")
        return

    # Mark the firm as processed
    processed_firms[firm_seal_key] = True
    result_counter.value += 1

    seal_date_str = seal_date.strftime(CONFIG.SEAL_CHANGE_DATE_PATTERN)
    logger.info(f"Processing seal firm {result_counter.value}: {haendler_bez} for seal date: {seal_date_str}")

    free_up_memory_and_drop_table(db, 'angebot')
    free_up_memory_and_drop_table(db, 'clicks')

    initialize_clicks_table(db)
    initialize_offer_table(db)
    load_selection_criteria_inflow_angebot_data(db, seal_date_str)
    load_selection_criteria_inflow_click_data(db, seal_date_str)

    products = clicks_service.get_top_n_products_by_clicks(haendler_bez, seal_date_str)
    logger.info(f"Sampled {len(products)} products for {haendler_bez}.")

    filtered_products = product_service.filter_continuously_offered_products(
        haendler_bez, products, seal_date_str,
        week_amount=CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT
    )
    logger.info(f"Filtered {len(filtered_products)} products for firm {haendler_bez}.")

    tasks = [task for product in filtered_products for task in
             gather_tasks(product, seal_date_str, seal_firms, geizhals_id, allowed_firms, product_service)]

    with open('results.csv', 'a') as csvfile:
        for task in tasks:
            results = process_task(task)
            write_results_to_csv(results, csvfile)


def write_results_to_csv(results, csvfile):
    """Write processed results to CSV."""
    for row in results:
        csvfile.write(
            f"{row['produkt_id']},{row['haendler_bez']},{row['week_running_var']},{row['firm_has_seal_j']}\n"
        )
        csvfile.flush()


def calculate_index_space(db: DuckDBDataSource, seal_firms: pl.DataFrame, allowed_firms: pl.DataFrame,
                          seal_change_firms: pl.DataFrame, parallel=False):
    """Main function to calculate index space across multiple seal firms."""
    logger.info("Starting index space calculation.")

    with Manager() as manager:
        processed_firms = manager.dict()  # thread-safe dict
        result_counter = Value('i', 0)  # thread-safe counter

        # Prepare necessary arguments
        seal_firm_data_list = [
            (
                row['RESULTING MATCH'],
                row['RESULTING MATCH'],
                row['Guetesiegel First Date'],
                seal_firms.to_series(2),  # all seal firms, not allowed as counterfactuals
                allowed_firms.to_series(1),  # filtered allowed firms as counterfactual firms
                processed_firms,  # thread-safe dict
                Factory.create_offers_service(),
                Factory.create_clicks_service()
            )
            for row in seal_change_firms.iter_rows(named=True)
        ]

        logger.info("Processing seal firms...")
        with open('results.csv', 'w') as csvfile:
            csvfile.write("produkt_id,haendler_bez,week_running_var,firm_has_seal_j\n")

        if parallel:
            with ThreadPoolExecutor(max_workers=CONFIG.SPAWN_MAX_MAIN_PROCESSES_AMOUNT) as executor:
                logger.info(f"ThreadPoolExecutor max workers set to {executor._max_workers}")

                futures = [executor.submit(process_seal_firm, args, result_counter, db)
                           for args in seal_firm_data_list]

                for future in tqdm(as_completed(futures), total=len(futures), desc="Processing seal firms"):
                    future.result()
        else:
            with ThreadPoolExecutor(max_workers=1) as single_executor:
                futures = [single_executor.submit(process_seal_firm, args, result_counter, db)
                           for args in seal_firm_data_list]

                for future in tqdm(as_completed(futures), total=len(futures), desc="Processing seal firms"):
                    future.result()

    logger.info("Processing complete and results saved to results.csv.")


def main():
    """Main function to initialize database and start processing."""

    # Step 00
    db = DuckDBDataSource()
    db_initializer = DatabaseInitializer(db)
    db_initializer.initialize_database()

    logger.info("Verifying required tables in database.")
    required_tables = ['seal_change_firms', 'filtered_haendler_bez', 'products', 'retailers']
    for table in required_tables:
        result = db.queryAsPl(f"SELECT COUNT(*) FROM {table}")
        logger.info(f"Table {table} exists with {result[0][0]} rows.")

    # Step 01
    allowed_firms_repo = Factory.create_filtered_retailer_names_repository()
    seal_change_firms_repo = Factory.create_seal_change_firms_repository()

    allowed_firms = allowed_firms_repo.fetch_all_filtered_retailers()
    seal_change_firms = seal_change_firms_repo.fetch_all()

    # Step 02
    # Start thread monitoring in a separate thread
    monitoring_thread = threading.Thread(target=monitor_prcss_threads, daemon=True)
    monitoring_thread.start()

    calculate_index_space(db, seal_change_firms, allowed_firms, seal_change_firms,
                          parallel=False)


if __name__ == '__main__':
    os.environ["POLARS_MAX_THREADS"] = "64"
    os.environ["NUMEXPR_MAX_THREADS"] = "32"
    os.environ["ARROW_NUM_THREADS"] = "8"

    main()

# current number of threads: 245
# processing seal firms:   0%|          | 0/296 [00:00<?, ?it/s]memory usage: 164.27 mb
# current number of threads: 237