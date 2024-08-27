import logging
import datetime as dt
import random
import pandas as pd
import dask.dataframe as dd
from dask import delayed, compute
from functools import lru_cache
import gc
from CONFIG import *

logger = logging.getLogger(__name__)

@lru_cache(maxsize=None)
def calculate_running_var_t_from_u(unix_time):
    if isinstance(unix_time, str):
        unix_time = date_to_unix_time(unix_time)
    return int((unix_time - UNIX_TIME_ORIGIN) / UNIX_WEEK)

@lru_cache(maxsize=None)
def date_to_unix_time(date_str):
    try:
        date_obj = dt.datetime.strptime(date_str, '%d.%m.%Y')
        return int(date_obj.timestamp())
    except ValueError as e:
        logger.error(f"Error parsing date: {e}")
        return None


def load_data():
    """
    Loads the necessary data files for processing.

    Returns:
        tuple: Contains Pandas DataFrames for seal change firms, filtered handler,
               products, and retailers.
    """
    logger.info("Loading data...")

    seal_change_firms = pd.read_csv(SEAL_CHANGE_FIRMS, delimiter=";", dtype={'Guetesiegel First Date': 'str'})
    filtered_haendler_bez = pd.read_csv(FILTERED_HAENDLER_BEZ, dtype={'haendler_bez': 'category'})

    # Load only necessary columns with dask for large datasets
    products_df = dd.read_parquet(PARQUE_FILES_DIR / 'produkt.parquet', columns=['produkt_id'])
    retailers_df = dd.read_parquet(PARQUE_FILES_DIR / 'haendler.parquet', columns=['haendler_bez'])

    logger.info("Data loaded successfully.")
    return seal_change_firms, filtered_haendler_bez, products_df, retailers_df

def select_seal_change_firms(seal_change_firms):
    """
    Filters the seal change firms DataFrame to select only those rows where
    'Guetesiegel First Date' is not null and has the correct date format.

    Args:
        seal_change_firms (pd.DataFrame): The DataFrame containing seal change firm data.

    Returns:
        pd.DataFrame: Filtered DataFrame with valid 'Guetesiegel First Date'.
    """
    logger.info("Selecting seal change firms...")
    date_pattern = r'^\d{2}\.\d{2}\.\d{4}$'
    filtered_df = seal_change_firms[
        seal_change_firms['Guetesiegel First Date'].notnull() &
        seal_change_firms['Guetesiegel First Date'].str.contains(date_pattern)
    ]
    logger.info(f"Filtered {len(filtered_df)} firms with valid 'Guetesiegel First Date'.")
    
    # Cleanup memory
    # del seal_change_firms
    # gc.collect()

    return filtered_df

def get_top_n_products_by_clicks(haendler_bez, seal_date_str, clicks_data, top_n=TOP_PRODUCTS_OF_SEAL_CHANGE_FIRM_BY_CLICKS_AMOUNT):
    """
    Retrieves the top N products by clicks for a given firm around the seal change date.

    Args:
        haendler_bez (str): The identifier of the firm.
        seal_date_str (str): The seal change date as a string in 'DD.MM.YYYY' format.
        clicks_data (dask.dataframe.DataFrame): The Dask DataFrame containing click data.
        top_n (int, optional): Number of top products to return. Defaults to a value from CONFIG.

    Returns:
        list: List of product IDs of the top products by clicks.
    """
    logger.info(f"Getting top {top_n} products by clicks for firm {haendler_bez} around the seal date {seal_date_str}.")
    seal_date = dt.datetime.strptime(seal_date_str, '%d.%m.%Y')

    observation_start = seal_date - dt.timedelta(weeks=HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT)
    observation_end = seal_date + dt.timedelta(weeks=HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT)

    observation_start_unix = int(observation_start.timestamp())
    observation_end_unix = int(observation_end.timestamp())

    # Filter and group by only necessary columns using Dask for lazy evaluation
    clicks_data_firm = clicks_data[
        (clicks_data['haendler_bez'] == haendler_bez) &
        (clicks_data['timestamp'] >= observation_start_unix) &
        (clicks_data['timestamp'] <= observation_end_unix)
    ]

    try:
        top_products = clicks_data_firm.groupby('produkt_id').size().nlargest(top_n).compute()
        logger.info(f"Top products found: {top_products.index.tolist()}")

        # Cleanup memory
        del clicks_data_firm, clicks_data, seal_date, observation_start, observation_end
        gc.collect()

        return top_products.index.tolist()
    except Exception as e:
        logger.error(f"An error occurred while computing top products: {str(e)}")

        # Cleanup memory
        del clicks_data_firm, clicks_data, seal_date, observation_start, observation_end
        gc.collect()

        return []


def get_start_of_week(date):
    """
    Returns the start of the week (Monday) for a given date.

    Args:
        date (datetime): The date to find the start of the week for.

    Returns:
        datetime: The datetime representing the start of the week.
    """
    # logger.info(f"Getting start of the week for date: {date}")
    start_of_week = date - dt.timedelta(days=date.weekday())
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    # logger.info(f"Start of the week: {start_of_week}")
    return start_of_week

def date_to_unix_time(date_str):
    """
    Converts a date string in 'DD.MM.YYYY' format to Unix time.

    Args:
        date_str (str): Date string to convert.

    Returns:
        int: Unix time corresponding to the date string.
        None: If the date string is invalid.
    """
    logger.info(f"Converting date string {date_str} to Unix time.")
    try:
        date_obj = dt.datetime.strptime(date_str, '%d.%m.%Y')
        unix_time = int(date_obj.timestamp())
        # logger.info(f"Converted Unix time: {unix_time}")
        return unix_time
    except ValueError as e:
        logger.error(f"Error parsing date: {e}")
        return None

def is_product_continuously_offered(produkt_id, haendler_bez, seal_date_str, angebot_data, weeks):
    """
    Checks if a product is continuously offered by a firm around a seal change date,
    allowing for at most one week with missing offers entirely.

    Args:
        produkt_id (str): The product ID to check.
        haendler_bez (str): The identifier of the firm.
        seal_date_str (str): The seal change date as a string.
        angebot_data (dask.dataframe.DataFrame): The Dask DataFrame containing offer data.
        weeks (int): Number of weeks before and after the seal date to check.

    Returns:
        bool: True if the product is continuously offered, False otherwise.
    """
    logger.info(f"Starting check for product {produkt_id} offered by firm {haendler_bez} around the seal date {seal_date_str}.")
    
    if weeks < 4:
        logger.warning("Weeks parameter is less than 4. Not enough weeks to determine continuous offer. Returning False.")
        return False

    # Convert seal_date to Unix time
    
    seal_date = date_to_unix_time(seal_date_str)
    seal_date_start_of_week = get_start_of_week(dt.datetime.fromtimestamp(seal_date))
    # logger.info(f"Seal date: {seal_date}, aligned to the start of the week: {seal_date_start_of_week}")

    offered_period_start = seal_date_start_of_week - dt.timedelta(weeks=weeks)
    offered_period_end = seal_date_start_of_week + dt.timedelta(weeks=weeks + 1)
    # logger.info(f"Checking offers from {offered_period_start} to {offered_period_end}.")

    angebot_data_filtered_haendler_produkt = angebot_data[
        (angebot_data['produkt_id'] == produkt_id) &
        (angebot_data['haendler_bez'] == haendler_bez)
    ].copy()

    # angebot_data_filtered['dtimebegin'] = angebot_data_filtered['dtimebegin'].apply(lambda x: dt.datetime.fromtimestamp(x))
    # angebot_data_filtered['dtimeend'] = angebot_data_filtered['dtimeend'].apply(lambda x: dt.datetime.fromtimestamp(x))

    logger.info(f"Filtered data contains {len(angebot_data_filtered_haendler_produkt)} rows for product {produkt_id} from firm {haendler_bez}.")

    offered_weeks = set()

    for index, row in angebot_data_filtered_haendler_produkt.iterrows():
        start_date = row['dtimebegin']
        end_date = row['dtimeend']

        current_date = start_date
        while current_date <= end_date:
            if offered_period_start <= current_date < offered_period_end:
                week_number = current_date.isocalendar()[1]
                offered_weeks.add(week_number)
            current_date += dt.timedelta(days=1)

    weeks_with_no_offers = 0

    for i in range(weeks * 2 + 1):
        week_start = offered_period_start + dt.timedelta(weeks=i)
        week_number = week_start.isocalendar()[1]

        if week_number not in offered_weeks:
            weeks_with_no_offers += 1
            logger.info(f"Week {week_number} has no offers. Weeks without offers so far: {weeks_with_no_offers}")
            if weeks_with_no_offers > 1:
                logger.info("Product is not continuously offered. Exceeded the allowed number of weeks with no offers. Returning False.")
                return False

    # Cleanup memory
    del angebot_data_filtered_haendler_produkt
    gc.collect()

    logger.info("Product is continuously offered within the specified period. Returning True.")
    return True

def filter_continuously_offered_products(haendler_bez, top_products, seal_date_str, angebot_data, week_amount):
    """
    Filters the top products to find those that are continuously offered
    by the firm around the seal change date.

    Args:
        haendler_bez (str): The identifier of the firm.
        top_products (list): List of product IDs to filter.
        seal_date_str (str): The seal change date as a string.
        angebot_data (dask.dataframe.DataFrame): The Dask DataFrame containing offer data.
        week_amount (int): Number of weeks before and after the seal date to check.

    Returns:
        list: List of product IDs that are continuously offered.
    """
    logger.info(f"Filtering continuously offered products for firm {haendler_bez} and seal date {seal_date_str}.")
    continuously_offered_products = []

    # Convert seal_date to Unix time
    seal_date = date_to_unix_time(seal_date_str)

    seal_date_start_of_week = get_start_of_week(dt.datetime.fromtimestamp(seal_date))
    logger.info(f"Seal date: {seal_date}, aligned to the start of the week: {seal_date_start_of_week}")

    offered_period_start = seal_date_start_of_week - dt.timedelta(weeks=week_amount)
    offered_period_end = seal_date_start_of_week + dt.timedelta(weeks=week_amount + 1)
    logger.info(f"Checking offers from {offered_period_start} to {offered_period_end}.")

    angebot_data_filtered_haendler = angebot_data[
        (angebot_data['haendler_bez'] == haendler_bez)
    ].compute()

    angebot_data_filtered_haendler['dtimebegin'] = angebot_data_filtered_haendler['dtimebegin'].apply(lambda x: dt.datetime.fromtimestamp(x))
    angebot_data_filtered_haendler['dtimeend'] = angebot_data_filtered_haendler['dtimeend'].apply(lambda x: dt.datetime.fromtimestamp(x))


    for produkt_id in top_products:
        # Pass the filtered angebot data directly to is_product_continuously_offered
        if is_product_continuously_offered(produkt_id, haendler_bez, seal_date_str, angebot_data_filtered_haendler, week_amount):
            continuously_offered_products.append(produkt_id)

    # Cleanup memory
    del angebot_data_filtered_haendler
    gc.collect()

    logger.info(f"Continuously offered products: {continuously_offered_products}")
    return continuously_offered_products

def get_rand_max_20_counterfactual_firms(product_id, seal_date_str, angebot_data, seal_firms, allowed_firms):
    """
    Retrieves up to 20 counterfactual firms that offered a specific product
    at the time of the seal date but were not part of the seal firms.

    Args:
        product_id (str): The product ID to check.
        seal_date_str (str): The seal change date as a string.
        angebot_data (dask.dataframe.DataFrame): The Dask DataFrame containing offer data.
        seal_firms (list): List of firms that received the seal.

    Returns:
        list: List of counterfactual firms.
    """
    logger.info(f"Getting random max 20 counterfactual firms for product {product_id} and seal date {seal_date_str}.")
    
    # Convert seal_date to Unix time
    seal_date = date_to_unix_time(seal_date_str)

    offered_at_seal_time = angebot_data[
        (angebot_data['produkt_id'] == product_id) &
        (angebot_data['dtimebegin'] <= seal_date) &
        (angebot_data['dtimeend'] >= seal_date)
    ].compute()

    firms_offering_product = offered_at_seal_time['haendler_bez'].unique().tolist()

    counterfactual_firms = [firm for firm in firms_offering_product if firm not in seal_firms and firm in allowed_firms]
    
    if len(counterfactual_firms) > 15:
        counterfactual_firms = random.sample(counterfactual_firms, 15)
    
    logger.info(f"Counterfactual firms: {counterfactual_firms}")

    # Cleanup memory
    del offered_at_seal_time, firms_offering_product
    gc.collect()

    return counterfactual_firms

def get_offered_weeks(angebot_data, prod_id, firm_id, seal_date_str):
    """
    Retrieves the weeks during which a product was offered by a firm,
    within a restricted observation window based on the seal date.

    Args:
        angebot_data (dask.dataframe.DataFrame): The Dask DataFrame containing offer data.
        prod_id (str): The product ID to check.
        firm_id (str): The identifier of the firm.
        seal_date_str (str): The seal change date as a string.

    Returns:
        set: Set of week numbers during which the product was offered.
    """
    logger.info(f"Getting offered weeks for product {prod_id}, firm {firm_id}, and seal date {seal_date_str}.")
    
    # Convert seal_date to Unix time
    seal_date = date_to_unix_time(seal_date_str)
    seal_date_dt = dt.datetime.fromtimestamp(seal_date)

    start_year = seal_date_dt.year - 1
    end_year = seal_date_dt.year + 1

    observation_start = dt.datetime(start_year, 1, 1)
    observation_end = dt.datetime(end_year, 12, 31, 23, 59, 59)

    filtered_angebot = angebot_data[
        (angebot_data['produkt_id'] == prod_id) &
        (angebot_data['haendler_bez'] == firm_id) &
        (angebot_data['dtimebegin'] <= observation_end.timestamp()) &
        (angebot_data['dtimeend'] >= observation_start.timestamp())
    ].compute()

    filtered_angebot['dtimebegin'] = pd.to_datetime(filtered_angebot['dtimebegin'], unit='s')
    filtered_angebot['dtimeend'] = pd.to_datetime(filtered_angebot['dtimeend'], unit='s')

    offered_weeks = set()

    for _, row in filtered_angebot.iterrows():
        start_date = row['dtimebegin']
        end_date = row['dtimeend']

        if start_date < observation_start:
            start_date = observation_start
        if end_date > observation_end:
            end_date = observation_end

        current_date = start_date
        while current_date <= end_date:
            current_unix_time = int(current_date.timestamp())
            week_number = int(calculate_running_var_t_from_u(current_unix_time))
            offered_weeks.add(week_number)
            current_date += dt.timedelta(days=1)

    logger.info(f"Offered weeks: {offered_weeks}")

    # Cleanup memory
    del filtered_angebot
    gc.collect()

    return offered_weeks
