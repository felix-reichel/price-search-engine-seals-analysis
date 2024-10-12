import logging
import time
from functools import lru_cache
from multiprocessing import Manager, Pool, Value

from tqdm import tqdm

import CONFIG
from impl.db.datasource import DuckDBDataSource
from impl.db.loaders.init_db import DatabaseInitializer
from impl.db.loaders.load_temp_clicks_data import initialize_clicks_table, load_click_data
from impl.db.loaders.load_temp_offers_data import initialize_offer_table, load_angebot_data
from impl.factories.service_factory import ServiceFactory
from impl.helpers import calculate_running_var_t_from_u, print_process_mem_usage

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    filename='01_calculate_index_space.log', filemode='w')
logger = logging.getLogger(__name__)


def free_up_prev_inflow_tables(db: DuckDBDataSource, table_name):
    print_process_mem_usage()

    db.free_up_table_and_manipulate_file_logs(table_name)

    print(f"Table '{table_name}' dropped to free memory. Going to sleep now...zzZ")

    time.sleep(60)
    print_process_mem_usage()


@lru_cache(maxsize=None)
def calculate_running_var_t_from_u_cached(unix_time):
    return calculate_running_var_t_from_u(unix_time)


def process_single_product(product, firm_id, seal_date_str, product_service, has_seal):
    offered_weeks = product_service.get_offered_weeks(product, firm_id, seal_date_str)
    return [{'produkt_id': product, 'haendler_bez': firm_id, 'week_running_var': week, 'firm_has_seal_j': has_seal}
            for week in offered_weeks]


def gather_tasks(product, seal_date_str, seal_firms, geizhals_id, allowed_firms, product_service):
    tasks = [(product, geizhals_id, seal_date_str, True, product_service)]
    # logger.info(f"Main task for product {product}: {geizhals_id}")

    counterfactual_firms = product_service.get_rand_max_N_counterfactual_firms(product, seal_date_str, seal_firms,
                                                                               allowed_firms)
    # logger.info(f"Counterfactual firms for product {product}: {counterfactual_firms}")

    tasks += [(product, firm, seal_date_str, False, product_service) for firm in counterfactual_firms]

    logger.info(f"Total tasks amounted for selected seal change firm are: {len(tasks)}")
    return tasks


def process_task(args):
    product, firm, seal_date_str, is_main_firm, product_service = args
    # logger.info(f"Processing {'main' if is_main_firm else 'counterfactual'} firm task: {firm} for product {product}")
    return process_single_product(product, firm, seal_date_str, product_service, int(is_main_firm))


def process_seal_firm(seal_firm_data, result_counter, db: DuckDBDataSource):
    (
        haendler_bez, geizhals_id, seal_date, seal_firms,
        allowed_firms, processed_firms, lock, product_service, clicks_service
    ) = seal_firm_data

    with lock:
        if haendler_bez in processed_firms:
            logger.info(f"Firm {haendler_bez} has already been processed. Skipping.")
            return None
        processed_firms[haendler_bez] = True
        result_counter.value += 1
        firm_count = result_counter.value

    seal_date_str = seal_date.strftime(CONFIG.SEAL_CHANGE_DATE_PATTERN)

    logger.info(f"Processing seal firm {firm_count}: {haendler_bez} for seal date: {seal_date_str}")

    free_up_prev_inflow_tables(db, 'angebot')
    free_up_prev_inflow_tables(db, 'clicks')

    initialize_clicks_table(db)
    initialize_offer_table(db)

    load_angebot_data(db, seal_date_str)
    load_click_data(db, seal_date_str)

    products = clicks_service.get_top_n_products_by_clicks(haendler_bez, seal_date_str)

    logger.info(f"Sampled {len(products)} products for {haendler_bez}.")

    filtered_products = product_service.filter_continuously_offered_products(
        haendler_bez, products, seal_date_str, week_amount=CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT
    )

    logger.info(f"Filtered products meeting criteria: {len(filtered_products)}.")

    tasks = [task for product in filtered_products for task in
             gather_tasks(product, seal_date_str, seal_firms, geizhals_id, allowed_firms, product_service)]

    with open('results.csv', 'a') as csvfile:
        for task in tasks:
            results = process_task(task)

            # logger.info(f"Results of task {task}: {results}")

            write_results_to_csv(results, csvfile)

    return True


def write_results_to_csv(results, csvfile):
    for row in results:
        csvfile.write(
            f"{row['produkt_id']},{row['haendler_bez']},{row['week_running_var']},{row['firm_has_seal_j']}\n"
        )
        csvfile.flush()


def calculate_index_space(parallel=False):
    logger.info("Starting main process.")

    with Manager() as manager:
        processed_firms = manager.dict()
        lock = manager.Lock()
        result_counter = Value('i', 0)

        seal_firm_data_list = [
            (
                haendler_bez,
                seal_change_firms['RESULTING MATCH'][i],
                seal_change_firms['Guetesiegel First Date'][i],
                seal_firms,
                allowed_firms,
                processed_firms,
                lock,
                ServiceFactory.create_offers_service(),
                ServiceFactory.create_clicks_service()
            )
            for i, haendler_bez in enumerate(seal_change_firms['RESULTING MATCH'])
        ]

        logger.info("Processing seal firms...")

        with open('results.csv', 'w') as csvfile:
            csvfile.write("produkt_id,haendler_bez,week_running_var,firm_has_seal_j\n")

        if parallel:
            with Pool(processes=CONFIG.SPAWN_MAX_MAIN_PROCESSES_AMOUNT) as pool:
                list(tqdm(
                    pool.imap_unordered(lambda args: process_seal_firm(args, result_counter, db), seal_firm_data_list),
                    total=len(seal_firm_data_list), desc="Processing seal firms"))
        else:
            for data in tqdm(seal_firm_data_list, total=len(seal_firm_data_list), desc="Processing seal firms"):
                process_seal_firm(data, result_counter, db)

    logger.info("Processing complete and results saved to results.csv.")


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

    # Step 02 - init global seal params
    # Using repositories directly, as they don't have corresponding services

    filtered_retailer_names_repo = ServiceFactory.create_filtered_retailer_names_repository()
    seal_change_firms_repo = ServiceFactory.create_seal_change_firms_repository()

    allowed_firms = filtered_retailer_names_repo.fetch_all_filtered_retailers()
    seal_change_firms = seal_change_firms_repo.fetch_all()

    seal_firms = seal_change_firms

    # Step 03 - calculate index space i,j,t

    calculate_index_space(parallel=False)
