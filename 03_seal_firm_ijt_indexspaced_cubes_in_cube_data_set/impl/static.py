import datetime as dt
import logging
import random

import polars as pl

from CONFIG import *
from impl.helpers import date_to_unix_time, calculate_running_var_t_from_u, get_start_of_week

logger = logging.getLogger(__name__)


# Function to load data using Polars
def load_data():
    logger.info("Loading data...")

    # Load the necessary CSV and Parquet files using Polars
    seal_change_firms = pl.read_csv(SEAL_CHANGE_FIRMS, separator=";")
    filtered_haendler_bez = pl.read_csv(FILTERED_HAENDLER_BEZ)

    # Load Parquet files with necessary columns using Polars
    products_df = pl.read_parquet(PARQUET_FILES_DIR / 'produkt.parquet', columns=['produkt_id'])
    retailers_df = pl.read_parquet(PARQUET_FILES_DIR / 'haendler.parquet', columns=['haendler_bez'])

    logger.info("Data loaded successfully.")
    return seal_change_firms, filtered_haendler_bez, products_df, retailers_df


# Function to filter seal change firms with valid 'Guetesiegel First Date'
def select_seal_change_firms(seal_change_firms):
    logger.info("Selecting seal change firms...")
    date_pattern = r'^\d{2}\.\d{2}\.\d{4}$'
    filtered_df = seal_change_firms.filter(
        (seal_change_firms['Guetesiegel First Date'].is_not_null()) &
        (seal_change_firms['Guetesiegel First Date'].str.contains(date_pattern))
    )
    logger.info(f"Filtered {filtered_df.height} firms with valid 'Guetesiegel First Date'.")
    return filtered_df


# Function to get the top N products by clicks using Polars
@DeprecationWarning
def get_top_n_products_by_clicks(haendler_bez, seal_date_str, clicks_data,
                                 top_n=TOP_PRODUCTS_OF_SEAL_CHANGE_FIRM_BY_CLICKS_AMOUNT,
                                 time_window_around_seal=HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT):
    logger.info(f"Getting top {top_n} products by clicks for firm {haendler_bez} around the seal date {seal_date_str}.")
    seal_date = dt.datetime.strptime(seal_date_str, '%d.%m.%Y')

    # Define observation period
    observation_start = seal_date - dt.timedelta(weeks=time_window_around_seal)
    observation_end = seal_date + dt.timedelta(weeks=time_window_around_seal)

    observation_start_unix = int(observation_start.timestamp())
    observation_end_unix = int(observation_end.timestamp())

    if not isinstance(clicks_data, pl.DataFrame):
        clicks_data = pl.DataFrame(clicks_data)

    # Filter Polars DataFrame and group by product
    clicks_data_firm = clicks_data.filter(
        (clicks_data['haendler_bez'] == haendler_bez) &
        (clicks_data['timestamp'] >= observation_start_unix) &
        (clicks_data['timestamp'] <= observation_end_unix)
    )

    try:
        top_products_df = clicks_data_firm.group_by('produkt_id').agg(
            pl.col('produkt_id').count().alias('count')
        ).sort('count', descending=True).head(top_n)

        top_products = top_products_df['produkt_id'].to_list()

        logger.info(f"Top products found: {top_products}")

        return top_products
    except Exception as e:
        logger.error(f"An error occurred while computing top products: {str(e)}")
        return []


# Function to draw N random products deterministically
@DeprecationWarning
def get_random_n_products_deterministic(haendler_bez, angebot_data, seal_date_str,
                                        n=TOP_PRODUCTS_OF_SEAL_CHANGE_FIRM_BY_CLICKS_AMOUNT,
                                        seed=RANDOM_SAMPLER_DETERMINISTIC_SEED):
    logger.info(f"Drawing {n} random products for firm {haendler_bez} deterministically.")

    # Filter offer data for the given firm
    angebot_data_filtered = angebot_data.filter(
        pl.col('haendler_bez') == haendler_bez
    )

    # Extract unique product IDs for the firm
    product_ids = angebot_data_filtered['produkt_id'].unique().to_list()

    if len(product_ids) == 0:
        logger.warning(f"No products found for firm {haendler_bez}. Returning empty list.")
        return []

    # Seed the random generator for deterministic behavior
    random.seed(seed)

    # Randomly sample N products or less if there aren't enough products
    random_products = random.sample(product_ids, min(n, len(product_ids)))

    logger.info(f"Random products drawn: {random_products}")

    return random_products


# Function to check if a product is continuously offered around the seal change date
@DeprecationWarning
def is_product_continuously_offered(produkt_id, haendler_bez, seal_date_str, angebot_data, weeks):
    logger.info(
        f"Checking if product {produkt_id} is continuously offered by firm {haendler_bez} around seal date {seal_date_str}.")

    if weeks < 4:
        logger.warning("Weeks parameter is less than 4. Not enough weeks to determine continuous offer.")
        return False

    seal_date = date_to_unix_time(seal_date_str)
    seal_date_start_of_week = get_start_of_week(dt.datetime.fromtimestamp(seal_date))

    offered_period_start = seal_date_start_of_week - dt.timedelta(
        weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED)
    offered_period_end = seal_date_start_of_week + dt.timedelta(
        weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED + 1)

    angebot_data_filtered = angebot_data.filter(
        (angebot_data['produkt_id'] == produkt_id) &
        (angebot_data['haendler_bez'] == haendler_bez)
    )

    offered_weeks = set()
    for row in angebot_data_filtered.iter_rows():

        if isinstance(row[2], dt.datetime):
            start_date = row[2]
            end_date = row[3]
        else:
            start_date = dt.datetime.fromtimestamp(row[2])  # dtimebegin
            end_date = dt.datetime.fromtimestamp(row[3])  # dtimeend

        # Iterate through weeks between start and end dates
        current_date = start_date
        while current_date <= end_date:
            if offered_period_start <= current_date < offered_period_end:
                week_number = current_date.isocalendar()[1]
                offered_weeks.add(week_number)
            current_date += dt.timedelta(days=1)

    min_offered_period_window_start = seal_date_start_of_week - dt.timedelta(
        weeks=weeks)

    # Check for missing weeks
    weeks_with_no_offers = 0
    for i in range(weeks * 2 + 1):
        week_start = min_offered_period_window_start + dt.timedelta(weeks=i)
        week_number = week_start.isocalendar()[1]

        if week_number not in offered_weeks:
            weeks_with_no_offers += 1
            logger.info(f"Week {week_number} has no offers. Weeks without offers so far: {weeks_with_no_offers}")
            if weeks_with_no_offers > 1:
                logger.info("Product is not continuously offered. Exceeded the allowed number of weeks with no offers.")
                return False

    logger.info("Product is continuously offered within the specified period.")
    return True


# Function to filter continuously offered products
@DeprecationWarning
def filter_continuously_offered_products(haendler_bez, top_products, seal_date_str, angebot_data, week_amount):
    logger.info(f"Filtering continuously offered products for firm {haendler_bez} and seal date {seal_date_str}.")
    continuously_offered_products = []

    angebot_data_filtered = angebot_data.filter(
        pl.col('haendler_bez') == haendler_bez
    )

    for produkt_id in top_products:
        if is_product_continuously_offered(produkt_id, haendler_bez, seal_date_str, angebot_data_filtered, week_amount):
            continuously_offered_products.append(produkt_id)

    logger.info(f"Continuously offered products: {continuously_offered_products}")
    return continuously_offered_products


# Function to retrieve up to 20 counterfactual firms using Polars
def get_rand_max_N_counterfactual_firms(product_id, seal_date_str, angebot_data, seal_firms, allowed_firms):
    logger.info(
        f"Getting random max {RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT} counterfactual firms for product {product_id} and seal date {seal_date_str}.")

    seal_date = date_to_unix_time(seal_date_str)
    seal_date_dt = dt.datetime.fromtimestamp(seal_date)

    offered_at_seal_time = angebot_data.filter(
        (angebot_data['produkt_id'] == product_id) &
        (angebot_data['dtimebegin'] <= seal_date_dt) &
        (angebot_data['dtimeend'] >= seal_date_dt)
    )

    print(offered_at_seal_time.select('haendler_bez').unique())

    firms_offering_product = offered_at_seal_time['haendler_bez']

    counterfactual_firms = firms_offering_product.filter(
        (~firms_offering_product.is_in(seal_firms)) & (firms_offering_product.is_in(allowed_firms))
    )

    counterfactual_firms_list = counterfactual_firms.to_list()

    # If there are more firms than the limit, sample RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT
    if len(counterfactual_firms_list) > RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT:
        random.seed(RANDOM_SAMPLER_DETERMINISTIC_SEED)
        counterfactual_firms_list = random.sample(counterfactual_firms_list, RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT)

    logger.info(f"Counterfactual firms: {counterfactual_firms_list}")
    return counterfactual_firms_list


# Function to retrieve offered weeks using Polars
@DeprecationWarning
def get_offered_weeks(angebot_data, prod_id, firm_id, seal_date_str):
    logger.info(f"Getting offered weeks for product {prod_id}, firm {firm_id}, and seal date {seal_date_str}.")

    seal_date = date_to_unix_time(seal_date_str)
    seal_date_dt = dt.datetime.fromtimestamp(seal_date)

    offer_time_spells_from = seal_date_dt - dt.timedelta(
        weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED)
    offer_time_spells_to = seal_date_dt + dt.timedelta(weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED)

    filtered_angebot = angebot_data.filter(
        (angebot_data['produkt_id'] == prod_id) &
        (angebot_data['haendler_bez'] == firm_id) &
        (angebot_data['dtimebegin'] <= offer_time_spells_to.timestamp())
        # & (angebot_data['dtimeend'] >= offer_time_spells_from.timestamp())
    )

    offered_weeks = set()
    for row in filtered_angebot.iter_rows():
        start_date = dt.datetime.fromtimestamp(row[2])  # dtimebegin
        end_date = dt.datetime.fromtimestamp(row[3])  # dtimeend

        # Adjust start and end dates within the offer period
        if start_date < offer_time_spells_from:
            start_date = offer_time_spells_from
        if end_date > offer_time_spells_to:
            end_date = offer_time_spells_to

        current_date = start_date
        while current_date <= end_date:
            week_number = calculate_running_var_t_from_u(int(current_date.timestamp()))
            current_date += dt.timedelta(weeks=1)
            offered_weeks.add(week_number)

    lower_bound: int = calculate_running_var_t_from_u(seal_date - 26 * UNIX_WEEK)
    upper_bound: int = calculate_running_var_t_from_u(seal_date + 26 * UNIX_WEEK)

    bounded_offered_weeks = {
        week for week in offered_weeks
        if lower_bound <= week <= upper_bound
    }

    logger.info(f"Filtered offered weeks: {bounded_offered_weeks}")

    return bounded_offered_weeks


