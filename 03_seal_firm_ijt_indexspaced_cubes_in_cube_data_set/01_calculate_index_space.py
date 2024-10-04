import logging
from functools import lru_cache
from multiprocessing import Manager, Pool, Value

from tqdm import tqdm

import CONFIG
from impl.db.datasource import DuckDBDataSource
from impl.helpers import calculate_running_var_t_from_u
from impl.loaders.init_db import DatabaseInitializer
from impl.loaders.load_temp_offers_data import load_angebot_data_v2, initialize_offer_table
from impl.repository.FilteredRetailerNamesRepository import FilteredRetailerNamesRepository
from impl.repository.OffersRepository import OffersRepository
from impl.repository.SealChangeFirmRepository import SealChangeFirmsDataRepository
from impl.service.OffersService import OffersService

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='01_calculate_index_space.log',
    filemode='w'
)

logger = logging.getLogger(__name__)


@lru_cache(maxsize=None)
def calculate_running_var_t_from_u_cached(unix_time):
    return calculate_running_var_t_from_u(unix_time)


def process_main_firm_single_product(product, geizhals_id, seal_date_str, product_service):
    offered_weeks_seal_firm = product_service.get_offered_weeks(
        product,
        geizhals_id,
        seal_date_str
    )

    return [
        {
            'produkt_id': product,
            'haendler_bez': geizhals_id,
            'week_running_var': week,
            'firm_has_seal_j': 1
        }
        for week in offered_weeks_seal_firm
    ]


def process_counterfactual_firm_single_product(product, counterfactual_firm, seal_date_str, product_service):
    offered_weeks_counterfactual_firm = product_service.get_offered_weeks(
        product,
        counterfactual_firm,
        seal_date_str
    )

    return [
        {
            'produkt_id': product,
            'haendler_bez': counterfactual_firm,
            'week_running_var': week,
            'firm_has_seal_j': 0
        }
        for week in offered_weeks_counterfactual_firm
    ]


def gather_tasks_for_product(product, seal_date_str, seal_firms, geizhals_id, allowed_firms, product_service):
    tasks = [(product, geizhals_id, seal_date_str, True)]
    counterfactual_firms = product_service.get_rand_max_N_counterfactual_firms(
        product,
        seal_date_str,
        seal_firms,
        allowed_firms
    )
    tasks.extend(
        (product, counterfactual_firm, seal_date_str, False)
        for counterfactual_firm in counterfactual_firms
    )
    return tasks


def process_task(args):
    product, firm, seal_date_str, is_main_firm, product_service = args
    if is_main_firm:
        return process_main_firm_single_product(product, firm, seal_date_str, product_service)
    else:
        return process_counterfactual_firm_single_product(product, firm, seal_date_str, product_service)


def write_results_to_csv(results, csvfile):
    for row in results:
        csvfile.write(
            f"{row['produkt_id']},{row['haendler_bez']},{row['week_running_var']},{row['firm_has_seal_j']}\n"
        )
        csvfile.flush()


def process_seal_firm(seal_firm_data, result_counter, db):
    (
        haendler_bez, geizhals_id, seal_date, seal_firms,
        allowed_firms, processed_firms, lock, product_service
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

    initialize_offer_table(db)

    # load relevant angebot data into DuckDB
    load_angebot_data_v2(db, seal_date_str)

    random_products = product_service.get_random_n_products_deterministic(
        haendler_bez,
        seal_date_str,
        CONFIG.RANDOM_PRODUCTS_AMOUNTS,
        CONFIG.RANDOM_SAMPLER_DETERMINISTIC_SEED,
        CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT,
    )

    filtered_products = product_service.filter_continuously_offered_products(
        haendler_bez,
        random_products,
        seal_date_str,
        week_amount=CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT
    )

    tasks = []
    for product in filtered_products:
        tasks.extend(
            gather_tasks_for_product(
                product,
                seal_date_str,
                seal_firms,
                geizhals_id,
                allowed_firms,
                product_service
            )
        )

    with open('results.csv', 'a') as csvfile:
        for task in tasks:
            results = process_task(task)
            write_results_to_csv(results, csvfile)

    return True


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
                OffersService(OffersRepository(db))
            )
            for i, haendler_bez in enumerate(seal_change_firms['RESULTING MATCH'])
        ]

        logger.info("Processing seal firms...")

        with open('results.csv', 'w') as csvfile:
            csvfile.write("produkt_id,haendler_bez,week_running_var,firm_has_seal_j\n")

        if parallel:
            with Pool(processes=CONFIG.SPAWN_MAX_MAIN_PROCESSES_AMOUNT) as pool:
                for _ in tqdm(
                        pool.imap_unordered(
                            lambda args: process_seal_firm(args, result_counter, db),
                            seal_firm_data_list
                        ),
                        total=len(seal_firm_data_list), desc="Processing seal firms"):
                    pass
        else:
            for data in tqdm(seal_firm_data_list, total=len(seal_firm_data_list), desc="Processing seal firms"):
                process_seal_firm(data, result_counter, db)

    logger.info("Processing complete and results saved to results.csv.")


if __name__ == '__main__':
    # 00
    db = DuckDBDataSource()
    db_initializer = DatabaseInitializer(db)
    db_initializer.initialize_database()

    logger.info("Verifying that required tables are present.")
    required_tables = ['seal_change_firms', 'filtered_haendler_bez', 'products', 'retailers']

    for table in required_tables:
        result = db.query(f"SELECT COUNT(*) FROM {table}")
        logger.info(f"Table {table} exists with {result[0][0]} rows.")
    logger.info("Database initialization completed and tables verified.")

    # 01

    allowed_firms = (FilteredRetailerNamesRepository(db)
                     .fetch_all_filtered_retailers())

    seal_change_firms = SealChangeFirmsDataRepository(db).fetch_all()

    seal_firms = seal_change_firms

    calculate_index_space(parallel=False)
