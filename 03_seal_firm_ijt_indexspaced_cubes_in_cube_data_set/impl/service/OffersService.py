import datetime as dt
import logging
import random

import polars as pl

import CONFIG
from CONFIG import *
from impl.helpers import get_unix_offer_data_inflow_time_range_from_seal_date
from impl.repository.OffersRepository import OffersRepository
from impl.static import date_to_unix_time, calculate_running_var_t_from_u, get_start_of_week

logger = logging.getLogger(__name__)


class OffersService:
    def __init__(self,
                 repository: OffersRepository):
        self.repository = repository

    # TODO: Refactor ME
    def get_offered_weeks(
            self,
            prod_id: str,
            firm_id: str,
            seal_date_str: str):

        unix_time_spells_from, unix_time_spells_to \
            = get_unix_offer_data_inflow_time_range_from_seal_date(seal_date_str)

        angebot_data = self.repository.fetch_offered_weeks(
            prod_id, firm_id, unix_time_spells_from, unix_time_spells_to)

        offered_weeks = set()
        for row in angebot_data.iter_rows():
            dtimebegin = max(int(row[2]), unix_time_spells_from)
            dtimeend = min(int(row[3]), unix_time_spells_to)

            current_date = dtimebegin
            while current_date <= dtimeend:
                week_number = calculate_running_var_t_from_u(current_date)
                offered_weeks.add(week_number)
                current_date += UNIX_WEEK

        seal_date = date_to_unix_time(seal_date_str)
        lower_bound = calculate_running_var_t_from_u(seal_date - 26 * UNIX_WEEK)
        upper_bound = calculate_running_var_t_from_u(seal_date + 26 * UNIX_WEEK)

        return {week for week in offered_weeks if lower_bound <= week <= upper_bound}

    def get_rand_max_N_counterfactual_firms(
            self,
            product_id: str,
            seal_date_str: str,
            seal_firms: list,
            allowed_firms: list):

        seal_date = date_to_unix_time(seal_date_str)
        angebot_data = self.repository.fetch_counterfactual_firms(product_id, seal_date).to_series(0).to_list()
        
        counterfactual_firms = []
        for f in angebot_data:
            if f not in seal_firms: # and f in allowed_firms:
                counterfactual_firms.append(f)

        random.seed(RANDOM_SAMPLER_DETERMINISTIC_SEED)
        
        sample = random.sample(counterfactual_firms, min(len(counterfactual_firms), RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT))
        return sample

    def get_random_n_products_deterministic(self,
                                            haendler_bez: str,
                                            seal_date_str: str,
                                            n: int = CONFIG.RANDOM_PRODUCTS_AMOUNTS,
                                            seed: int = CONFIG.RANDOM_SAMPLER_DETERMINISTIC_SEED,
                                            time_window_around_seal: int = CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT):

        seal_date = dt.datetime.strptime(seal_date_str, CONFIG.SEAL_CHANGE_DATE_PATTERN)
        observation_start = seal_date - dt.timedelta(weeks=time_window_around_seal)
        observation_end = seal_date + dt.timedelta(weeks=time_window_around_seal)

        observation_start_unix = int(observation_start.timestamp())
        observation_end_unix = int(observation_end.timestamp())

        product_ids = self.repository.fetch_random_products(
            haendler_bez, observation_start_unix, observation_end_unix
        ).to_series(0).to_list()

        if not product_ids:
            return []

        random.seed(seed)
        return random.sample(product_ids, min(n, len(product_ids)))

    def is_product_continuously_offered(self,
                                        produkt_id: str,
                                        haendler_bez: str,
                                        seal_date_str: str,
                                        weeks: int = CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT) -> pl.DataFrame:

        if weeks < 4:
            return False

        seal_date = date_to_unix_time(seal_date_str)
        seal_date_start_of_week = get_start_of_week(dt.datetime.fromtimestamp(seal_date))

        offered_period_start = seal_date_start_of_week - dt.timedelta(weeks=weeks)
        offered_period_end = seal_date_start_of_week + dt.timedelta(weeks=weeks)

        offered_period_start_unix = int(offered_period_start.timestamp())
        offered_period_end_unix = int(offered_period_end.timestamp())

        angebot_data = self.repository.fetch_product_offer_data(
            produkt_id, haendler_bez, offered_period_start_unix, offered_period_end_unix
        )

        offered_weeks = set()
        for row in angebot_data.iter_rows():
            dtimebegin = dt.datetime.fromtimestamp(row[0])
            dtimeend = dt.datetime.fromtimestamp(row[1])

            current_date = dtimebegin
            while current_date <= dtimeend:
                if offered_period_start <= current_date <= offered_period_end:
                    week_number = current_date.isocalendar()[1]
                    offered_weeks.add(week_number)
                current_date += dt.timedelta(weeks=1)

        weeks_with_no_offers = 0
        for i in range(weeks * 2 + 1):
            week_start = offered_period_start + dt.timedelta(weeks=i)
            week_number = week_start.isocalendar()[1]

            if week_number not in offered_weeks:
                weeks_with_no_offers += 1
                if weeks_with_no_offers > 1:
                    return False

        return True

    def filter_continuously_offered_products(self,
                                             haendler_bez: str,
                                             top_products: list,
                                             seal_date_str: str,
                                             week_amount: int = CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT) -> \
            list[str]:
        continuously_offered_products = []

        for produkt_id in top_products:
            if self.is_product_continuously_offered(produkt_id, haendler_bez, seal_date_str, weeks=week_amount):
                continuously_offered_products.append(produkt_id)

        return continuously_offered_products
