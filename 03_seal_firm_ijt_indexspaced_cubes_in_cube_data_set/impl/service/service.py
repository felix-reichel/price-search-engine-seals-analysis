import random
import datetime as dt
from impl.repository.repository import ProductRepository
from impl.static import date_to_unix_time, calculate_running_var_t_from_u, get_start_of_week
from CONFIG import *
import logging

logger = logging.getLogger(__name__)


class ProductService:
    def __init__(self, repository: ProductRepository):
        self.repository = repository

    def validate_params(self, **params):
        for name, value in params.items():
            if value is None:
                raise ValueError(f"Parameter '{name}' must be provided.")

    def get_unix_time_range(self, seal_date_str):
        seal_date = date_to_unix_time(seal_date_str)
        seal_date_dt = dt.datetime.fromtimestamp(seal_date)

        offer_time_spells_from = seal_date_dt - dt.timedelta(
            weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED)
        offer_time_spells_to = seal_date_dt + dt.timedelta(
            weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED)

        return int(offer_time_spells_from.timestamp()), int(offer_time_spells_to.timestamp())

    def get_offered_weeks(self, prod_id, firm_id, seal_date_str):
        unix_time_spells_from, unix_time_spells_to = self.get_unix_time_range(seal_date_str)

        angebot_data = self.repository.get_offered_weeks(prod_id, firm_id, unix_time_spells_from, unix_time_spells_to)

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

    def get_rand_max_N_counterfactual_firms(self, product_id, seal_date_str, seal_firms, allowed_firms):
        seal_date = date_to_unix_time(seal_date_str)
        angebot_data = self.repository.get_counterfactual_firms(product_id, seal_date)

        firms_offering_product = angebot_data['haendler_bez'].to_list()
        counterfactual_firms = [f for f in firms_offering_product if f not in seal_firms and f in allowed_firms]

        # if len(counterfactual_firms) > RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT:
        random.seed(RANDOM_SAMPLER_DETERMINISTIC_SEED)
        counterfactual_firms = random.sample(counterfactual_firms, RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT)

        return counterfactual_firms

    def get_top_n_products_by_clicks(self, haendler_bez, seal_date_str, top_n=10):
        seal_date = dt.datetime.strptime(seal_date_str, '%d.%m.%Y')

        observation_start = seal_date - dt.timedelta(weeks=8)
        observation_end = seal_date + dt.timedelta(weeks=8)

        observation_start_unix = int(observation_start.timestamp())
        observation_end_unix = int(observation_end.timestamp())

        result = self.repository.get_top_n_products_by_clicks(
            haendler_bez, observation_start_unix, observation_end_unix, top_n
        )
        return result['produkt_id'].to_list()

    def get_random_n_products_deterministic(self, haendler_bez, seal_date_str, n, seed, time_window_around_seal):
        seal_date = dt.datetime.strptime(seal_date_str, '%d.%m.%Y')

        observation_start = seal_date - dt.timedelta(weeks=time_window_around_seal)
        observation_end = seal_date + dt.timedelta(weeks=time_window_around_seal)

        observation_start_unix = int(observation_start.timestamp())
        observation_end_unix = int(observation_end.timestamp())

        product_ids = self.repository.get_random_products(
            haendler_bez, observation_start_unix, observation_end_unix
        ).to_series(0).to_list()

        if len(product_ids) == 0:
            return []

        random.seed(seed)
        return random.sample(product_ids, min(n, len(product_ids)))

    def is_product_continuously_offered(self, produkt_id, haendler_bez, seal_date_str, weeks=4):
        if weeks < 4:
            return False

        seal_date = date_to_unix_time(seal_date_str)
        seal_date_start_of_week = get_start_of_week(dt.datetime.fromtimestamp(seal_date))

        offered_period_start = seal_date_start_of_week - dt.timedelta(weeks=weeks)
        offered_period_end = seal_date_start_of_week + dt.timedelta(weeks=weeks)

        offered_period_start_unix = int(offered_period_start.timestamp())
        offered_period_end_unix = int(offered_period_end.timestamp())

        angebot_data = self.repository.get_product_offer_data(
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
                current_date += dt.timedelta(days=1)

        weeks_with_no_offers = 0
        for i in range(weeks * 2 + 1):
            week_start = offered_period_start + dt.timedelta(weeks=i)
            week_number = week_start.isocalendar()[1]

            if week_number not in offered_weeks:
                weeks_with_no_offers += 1
                if weeks_with_no_offers > 1:
                    return False

        return True

    def filter_continuously_offered_products(self, haendler_bez, top_products, seal_date_str, week_amount=4):
        continuously_offered_products = []

        for produkt_id in top_products:
            if self.is_product_continuously_offered(produkt_id, haendler_bez, seal_date_str, weeks=week_amount):
                continuously_offered_products.append(produkt_id)

        return continuously_offered_products
