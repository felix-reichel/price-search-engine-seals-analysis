import random
import datetime as dt
import polars as pl
from CONFIG import *
from impl.db.query_builder import QueryBuilder
from impl.db.duckdb_data_source import DuckDBDataSource
from impl.static import date_to_unix_time, calculate_running_var_t_from_u
import logging

logger = logging.getLogger(__name__)


def validate_params(**params):
    for name, value in params.items():
        if value is None:
            raise ValueError(f"Parameter '{name}' must be provided.")


def get_unix_time_range(seal_date_str):
    seal_date = date_to_unix_time(seal_date_str)
    seal_date_dt = dt.datetime.fromtimestamp(seal_date)

    offer_time_spells_from = seal_date_dt - dt.timedelta(
        weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED)
    offer_time_spells_to = seal_date_dt + dt.timedelta(
        weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED)

    return int(offer_time_spells_from.timestamp()), int(offer_time_spells_to.timestamp())


def get_offered_weeks(db: DuckDBDataSource, prod_id, firm_id, seal_date_str):
    """
    Retrieves the offered weeks for a product (prod_id), firm (firm_id), and seal date.
    """
    validate_params(prod_id=prod_id, firm_id=firm_id, seal_date_str=seal_date_str)

    logger.info(f"Getting offered weeks for product {prod_id}, firm {firm_id}, and seal date {seal_date_str}.")

    unix_time_spells_from, unix_time_spells_to = get_unix_time_range(seal_date_str)

    query = QueryBuilder('angebot_temp') \
        .select(['produkt_id', 'haendler_bez', 'dtimebegin', 'dtimeend']) \
        .build_where_clause_i_j_t(prod_id, firm_id, unix_time_spells_from, unix_time_spells_to) \
        .build()

    result = db.query(query)
    filtered_angebot = pl.DataFrame(result)

    offered_weeks = set()
    for row in filtered_angebot.iter_rows():
        dtimebegin = max(int(row[2]), unix_time_spells_from)  # dtimebegin is the 3rd column (index 2)
        dtimeend = min(int(row[3]), unix_time_spells_to)  # dtimeend is the 4th column (index 3)

        current_date = dtimebegin
        while current_date <= dtimeend:
            week_number = calculate_running_var_t_from_u(current_date)
            offered_weeks.add(week_number)
            current_date += UNIX_WEEK

    seal_date = date_to_unix_time(seal_date_str)
    lower_bound = calculate_running_var_t_from_u(seal_date - 26 * UNIX_WEEK)
    upper_bound = calculate_running_var_t_from_u(seal_date + 26 * UNIX_WEEK)

    bounded_offered_weeks = {week for week in offered_weeks if lower_bound <= week <= upper_bound}

    logger.info(f"Filtered offered weeks: {bounded_offered_weeks}")
    return bounded_offered_weeks


def get_rand_max_N_counterfactual_firms(db: DuckDBDataSource, product_id, seal_date_str, seal_firms, allowed_firms):
    """
    Retrieves random max N counterfactual firms for a product that are neither in the seal firms nor excluded by allowed firms.
    """
    validate_params(product_id=product_id, seal_date_str=seal_date_str)

    logger.info(
        f"Getting random max {RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT} counterfactual firms for product {product_id}.")

    seal_date = date_to_unix_time(seal_date_str)
    seal_date_dt = dt.datetime.fromtimestamp(seal_date)

    query = QueryBuilder('angebot_temp') \
        .select('haendler_bez') \
        .where(f"produkt_id = '{product_id}'") \
        .where(f"dtimebegin <= {seal_date_dt.timestamp()} AND dtimeend >= {seal_date_dt.timestamp()}") \
        .build()

    result = db.query(query)
    result_df = pl.DataFrame(result)

    firms_offering_product = result_df['haendler_bez'].to_list()
    counterfactual_firms = [f for f in firms_offering_product if f not in seal_firms and f in allowed_firms]

    if len(counterfactual_firms) > RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT:
        random.seed(RANDOM_SAMPLER_DETERMINISTIC_SEED)
        counterfactual_firms = random.sample(counterfactual_firms, RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT)

    logger.info(f"Counterfactual firms: {counterfactual_firms}")
    return counterfactual_firms


def get_top_n_products_by_clicks(db: DuckDBDataSource, haendler_bez, seal_date_str, top_n=10):
    """
    Retrieves the top N products by clicks for a given retailer (haendler_bez) around the seal date.
    """
    validate_params(haendler_bez=haendler_bez, seal_date_str=seal_date_str)

    logger.info(f"Getting top {top_n} products by clicks for firm {haendler_bez} around the seal date {seal_date_str}.")

    seal_date = dt.datetime.strptime(seal_date_str, '%d.%m.%Y')

    observation_start = seal_date - dt.timedelta(weeks=8)
    observation_end = seal_date + dt.timedelta(weeks=8)

    observation_start_unix = int(observation_start.timestamp())
    observation_end_unix = int(observation_end.timestamp())

    query = QueryBuilder('clicks_temp') \
        .select(['produkt_id', 'COUNT(*) AS count']) \
        .where(f"haendler_bez = '{haendler_bez}'") \
        .where(f"timestamp >= {observation_start_unix} AND timestamp <= {observation_end_unix}") \
        .group_by('produkt_id') \
        .order_by('count', ascending=False) \
        .limit(top_n) \
        .build()

    result = db.query(query)
    top_products_df = pl.DataFrame(result)

    top_products = top_products_df['produkt_id'].to_list()

    logger.info(f"Top products found: {top_products}")
    return top_products


def get_random_n_products_deterministic(db, haendler_bez, seal_date, n, seed):
    query = f"""
        SELECT produkt_id 
        FROM angebot 
        WHERE haendler_bez = '{haendler_bez}'
    """
    result_df = db.query(query)
    product_ids = result_df['produkt_id'].tolist()

    if len(product_ids) == 0:
        return []

    # Use deterministic sampling
    random.seed(seed)
    return random.sample(product_ids, min(n, len(product_ids)))

