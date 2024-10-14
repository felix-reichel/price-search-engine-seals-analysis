import logging
import os
from functools import lru_cache
from multiprocessing import Manager, Pool, Value

import polars as pl
from tqdm import tqdm

import CONFIG
from CONFIG import (
    PARQUET_FILES_DIR, CLICKS_FOLDER, ANGEBOTE_FOLDER, SPAWN_MAX_MAIN_PROCESSES_AMOUNT
)
from static import (
    get_rand_max_N_counterfactual_firms, calculate_running_var_t_from_u,
    filter_continuously_offered_products,
    select_seal_change_firms, load_data, get_offered_weeks, get_random_n_products_deterministic
)
from impl.helpers import get_week_year_from_seal_date, generate_weeks_around_seal, file_exists_in_folders, \
    get_year_month_from_seal_date, generate_months_around_seal

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='process_log.log',
    filemode='w'
)
logger = logging.getLogger(__name__)

# Columns needed from angebot and clicks data
ANGEBOT_COLUMNS = ['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend']
CLICKS_COLUMNS = ['produkt_id', 'haendler_bez', 'timestamp']

# Set dtypes to minimize memory usage
ANGEBOT_DTYPE = {
    'produkt_id': pl.Int64,
    'haendler_bez': pl.Categorical,
    'dtimebegin': pl.Int64,
    'dtimeend': pl.Int64
}

CLICKS_DTYPE = {
    'produkt_id': pl.Int64,
    'haendler_bez': pl.Categorical,
    'timestamp': pl.Int64
}


@PendingDeprecationWarning
@lru_cache(maxsize=None)
def calculate_running_var_t_from_u_cached(unix_time):
    return calculate_running_var_t_from_u(unix_time)


@PendingDeprecationWarning
def load_relevant_angebot_data(seal_date, allowed_firms):
    seal_year, seal_week = get_week_year_from_seal_date(seal_date)
    relevant_files = generate_weeks_around_seal(seal_year, seal_week,
                                                CONFIG.OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED,
                                                CONFIG.OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED)
    df_list = []

    for file_name in relevant_files:
        file_path = file_exists_in_folders(file_name, ANGEBOTE_FOLDER)
        if file_path:
            df = pl.read_parquet(
                file_path,
                columns=ANGEBOT_COLUMNS,
                use_pyarrow=True
            )
            df_list.append(df)

    if not df_list:
        logger.warning(f"No relevant Angebot data found for seal date {seal_date}.")
        return None

    angebot_data = pl.concat(df_list)
    angebot_data_filtered = angebot_data.filter(pl.col("haendler_bez").is_in(allowed_firms))
    return angebot_data_filtered


@PendingDeprecationWarning
def load_relevant_click_data(seal_date):
    seal_year, seal_month = get_year_month_from_seal_date(seal_date)
    relevant_files = generate_months_around_seal(seal_year, seal_month)
    df_list = []

    for file_name in relevant_files:
        file_path = os.path.join(PARQUET_FILES_DIR, CLICKS_FOLDER, file_name)
        if os.path.isfile(file_path):
            df = pl.read_parquet(
                file_path,
                columns=CLICKS_COLUMNS,
                use_pyarrow=True
            )
            df_list.append(df)

    if not df_list:
        logger.warning(f"No relevant Click data found for seal date {seal_date}.")
        return None

    return pl.concat(df_list)


@PendingDeprecationWarning
def process_main_firm_single_product(product, geizhals_id, seal_date, angebot_data):
    main_product_results = [{
        'produkt_id': product,
        'haendler_bez': geizhals_id,
        'week_running_var': calculate_running_var_t_from_u_cached(seal_date),
        'firm_has_seal_j': 1
    }]

    offered_weeks_seal_firm = get_offered_weeks(angebot_data, product, geizhals_id, seal_date)
    for week in offered_weeks_seal_firm:
        if week != calculate_running_var_t_from_u_cached(seal_date):
            main_product_results.append({
                'produkt_id': product,
                'haendler_bez': geizhals_id,
                'week_running_var': week,
                'firm_has_seal_j': 1
            })

    return main_product_results


@PendingDeprecationWarning
def process_counterfactual_firm_single_product(product, counterfactual_firm, seal_date, angebot_data):
    counterfactual_results = [{
        'produkt_id': product,
        'haendler_bez': counterfactual_firm,
        'week_running_var': calculate_running_var_t_from_u_cached(seal_date),
        'firm_has_seal_j': 0
    }]

    offered_weeks_counterfactual_firm = get_offered_weeks(angebot_data, product, counterfactual_firm, seal_date)
    for week in offered_weeks_counterfactual_firm:
        if week != calculate_running_var_t_from_u_cached(seal_date):
            counterfactual_results.append({
                'produkt_id': product,
                'haendler_bez': counterfactual_firm,
                'week_running_var': week,
                'firm_has_seal_j': 0
            })

    return counterfactual_results


@PendingDeprecationWarning
def gather_tasks_for_product(product, seal_date, angebot_data, seal_firms, geizhals_id, allowed_firms):
    tasks = [(product, geizhals_id, seal_date, angebot_data, True)]
    counterfactual_firms = get_rand_max_N_counterfactual_firms(
        product, seal_date, angebot_data, seal_firms, allowed_firms
    )
    tasks.extend(
        (product, counterfactual_firm, seal_date, angebot_data, False)
        for counterfactual_firm in counterfactual_firms
    )
    return tasks


@PendingDeprecationWarning
def process_task(args):
    product, firm, seal_date, angebot_data, is_main_firm = args
    if is_main_firm:
        return process_main_firm_single_product(product, firm, seal_date, angebot_data)
    else:
        return process_counterfactual_firm_single_product(product, firm, seal_date, angebot_data)


@PendingDeprecationWarning
def process_seal_firm(seal_firm_data, result_counter):
    (
        haendler_bez, geizhals_id, seal_date, seal_firms,
        products_df, retailers_df, allowed_firms, processed_firms, lock
    ) = seal_firm_data

    with lock:
        result_counter.value += 1
        firm_count = result_counter.value
    logger.info(f"Processing seal firm {firm_count}: {haendler_bez} for seal date: {seal_date}")

    if haendler_bez in processed_firms:
        logger.info(f"Firm {firm_count}: {haendler_bez} has already been processed. Skipping.")
        return None  # Skip if this seal firm has already been processed

    with lock:
        processed_firms[haendler_bez] = True  # Mark this firm as processed

    angebot_data = load_relevant_angebot_data(seal_date, allowed_firms)
    clicks_data = load_relevant_click_data(seal_date)
    if angebot_data is None or clicks_data is None:
        return None

    # Get top products and filter
    # Old way using clicks data to get top N products
    # top_products = get_top_n_products_by_clicks(geizhals_id, seal_date, clicks_data, 50)

    # New way using random product selection
    top_products = get_random_n_products_deterministic(geizhals_id,
                                                       angebot_data,
                                                       seal_date,
                                                       CONFIG.RANDOM_PRODUCTS_AMOUNTS,
                                                       CONFIG.RANDOM_SAMPLER_DETERMINISTIC_SEED)

    filtered_products = filter_continuously_offered_products(
        haendler_bez, top_products, seal_date, angebot_data, 4
    )

    # Gather tasks
    tasks = []
    for product in filtered_products:
        tasks.extend(
            gather_tasks_for_product(product, seal_date, angebot_data, seal_firms, geizhals_id, allowed_firms)
        )

    with open('results.csv', 'a') as csvfile:
        for task in tasks:
            results = process_task(task)
            for row in results:
                csvfile.write(
                    f"{row['produkt_id']},{row['haendler_bez']},{row['week_running_var']},{row['firm_has_seal_j']}\n"
                )
            csvfile.flush()

    return True


@PendingDeprecationWarning
def main(parallel=False):
    logger.info("Starting main process.")

    seal_change_firms, filtered_haendler_bez, products_df, retailers_df = load_data()

    seal_change_firms = select_seal_change_firms(seal_change_firms)

    logger.info("Seal change firms selected.")

    allowed_firms = set(filtered_haendler_bez[:, 0].unique())

    logger.info(f"{len(allowed_firms)} number of allowed firms.")

    seal_firms = seal_change_firms['RESULTING MATCH'].unique().to_list()

    with Manager() as manager:
        processed_firms = manager.dict()
        lock = manager.Lock()
        result_counter = Value('i', 0)  # Shared counter for logging

        seal_firm_data = [
            (
                haendler_bez,
                seal_change_firms[i, 2],  # Geizhals identifier
                seal_change_firms[i, 3],  # Seal change date
                seal_firms,
                products_df,
                retailers_df,
                allowed_firms,
                processed_firms,
                lock
            )
            for i, haendler_bez in enumerate(seal_change_firms['RESULTING MATCH'])
        ]

        logger.info("Processing seal firms...")

        with open('results.csv', 'w') as csvfile:
            csvfile.write("produkt_id,haendler_bez,week_running_var,firm_has_seal_j\n")

        if parallel:
            with Pool(processes=SPAWN_MAX_MAIN_PROCESSES_AMOUNT) as pool:
                for _ in tqdm(
                        pool.imap_unordered(process_seal_firm, [(data, result_counter) for data in seal_firm_data]),
                        total=len(seal_firm_data), desc="Processing seal firms"):
                    pass
        else:
            for data in tqdm(seal_firm_data, total=len(seal_firm_data), desc="Processing seal firms"):
                process_seal_firm(data, result_counter)

    logger.info("Processing complete and results saved to results.csv.")


if __name__ == '__main__':
    pass
