import random

from impl.helpers import *
from impl.service.base.abc_service import AbstractBaseService

logger = logging.getLogger(__name__)


class OffersService(AbstractBaseService):

    def get_offered_weeks(self, prod_id: str, firm_id: str, seal_date_str: str) -> set:
        """
        Get the weeks during which a product was offered by a firm around the seal date.

        Parameters:
        prod_id (str): The product ID to filter by.
        firm_id (str): The firm ID to filter by.
        seal_date_str (str): The seal date as a string.

        Returns:
        set: A set of weeks in which the product was offered.
        """
        unix_time_spells_from, unix_time_spells_to = get_unix_offer_data_inflow_time_range_from_seal_date(seal_date_str)

        angebot_data = self.repository.fetch_offered_weeks(prod_id, firm_id, unix_time_spells_from, unix_time_spells_to)

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

    def get_rand_max_N_counterfactual_firms(self, product_id: str, seal_date_str: str, seal_firms: list, allowed_firms: list) -> list:
        """
        Get a random selection of counterfactual firms that offered the product around the seal date.

        Parameters:
        product_id (str): The product ID to filter by.
        seal_date_str (str): The seal date as a string.
        seal_firms (list): List of seal firms to exclude.
        allowed_firms (list): List of allowed firms.

        Returns:
        list: A random sample of counterfactual firms.
        """
        seal_date = date_to_unix_time(seal_date_str)
        angebot_data = self.repository.fetch_counterfactual_firms(product_id, seal_date).to_series(0).to_list()

        counterfactual_firms = [f for f in angebot_data if f not in seal_firms]

        random.seed(CONFIG.RANDOM_SAMPLER_DETERMINISTIC_SEED)
        sample = random.sample(counterfactual_firms, min(len(counterfactual_firms),
                                                         CONFIG.RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT))

        return sample

    def get_random_n_products_deterministic(self, haendler_bez: str, seal_date_str: str, n: int = CONFIG.RANDOM_PRODUCTS_AMOUNTS,
                                            seed: int = CONFIG.RANDOM_SAMPLER_DETERMINISTIC_SEED,
                                            time_window_around_seal: int = CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT) -> list:
        """
        Get a deterministic random sample of products offered by a retailer around the seal date.

        Parameters:
        haendler_bez (str): The retailer name.
        seal_date_str (str): The seal date as a string.
        n (int, optional): The number of products to return. Defaults to CONFIG.RANDOM_PRODUCTS_AMOUNTS.
        seed (int, optional): The seed for deterministic random sampling. Defaults to CONFIG.RANDOM_SAMPLER_DETERMINISTIC_SEED.
        time_window_around_seal (int, optional): Time window in weeks around the seal date. Defaults to CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT.

        Returns:
        list: A list of random product IDs.
        """
        seal_date = dt.datetime.strptime(seal_date_str, CONFIG.SEAL_CHANGE_DATE_PATTERN)
        observation_start = seal_date - dt.timedelta(weeks=time_window_around_seal)
        observation_end = seal_date + dt.timedelta(weeks=time_window_around_seal)

        observation_start_unix = int(observation_start.timestamp())
        observation_end_unix = int(observation_end.timestamp())

        product_ids = self.repository.fetch_random_products(haendler_bez, observation_start_unix, observation_end_unix).to_series(0).to_list()

        if not product_ids:
            return []

        random.seed(seed)
        return random.sample(product_ids, min(n, len(product_ids)))

    def is_product_continuously_offered(self, produkt_id: str, haendler_bez: str, seal_date_str: str, weeks: int = CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT) -> bool:
        """
        Check if a product was continuously offered by a retailer over a given period.

        Parameters:
        produkt_id (str): The product ID to check.
        haendler_bez (str): The retailer name.
        seal_date_str (str): The seal date as a string.
        weeks (int, optional): The number of weeks around the seal date to check. Defaults to CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT.

        Returns:
        bool: True if the product was continuously offered, False otherwise.
        """
        if weeks < 4:
            return False

        seal_date = date_to_unix_time(seal_date_str)
        seal_date_start_of_week = get_start_of_week(dt.datetime.fromtimestamp(seal_date))

        offered_period_start = seal_date_start_of_week - dt.timedelta(weeks=weeks)
        offered_period_end = seal_date_start_of_week + dt.timedelta(weeks=weeks)

        offered_period_start_unix = int(offered_period_start.timestamp())
        offered_period_end_unix = int(offered_period_end.timestamp())

        angebot_data = self.repository.fetch_product_offer_data(
            produkt_id, haendler_bez, offered_period_start_unix, offered_period_end_unix)

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

    def filter_continuously_offered_products(self, haendler_bez: str, top_products: list, seal_date_str: str,
                                             week_amount: int = CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT) -> list:
        """
        Filter and return products that were continuously offered by a retailer around the seal date.

        Parameters:
        haendler_bez (str): The retailer name.
        top_products (list): A list of top products to filter.
        seal_date_str (str): The seal date as a string.
        week_amount (int, optional): Number of weeks to check around the seal date. Defaults to CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT.

        Returns:
        list: A list of products that were continuously offered.
        """
        continuously_offered_products = []

        for produkt_id in top_products:
            if self.is_product_continuously_offered(produkt_id, haendler_bez, seal_date_str, weeks=week_amount):
                continuously_offered_products.append(produkt_id)

        return continuously_offered_products
