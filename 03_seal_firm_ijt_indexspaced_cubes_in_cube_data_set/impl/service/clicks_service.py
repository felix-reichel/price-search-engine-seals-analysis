import datetime as dt
import logging

import CONFIG
from impl.repository.clicks_repository import ClicksRepository
from impl.singleton import Singleton

logger = logging.getLogger(__name__)


class ClicksService(Singleton):
    def __init__(self, repository: ClicksRepository):
        if not hasattr(self, 'repository'):
            self.repository = repository

    def get_top_n_products_by_clicks(self, haendler_bez: str, seal_date_str: str, top_n: int = CONFIG.TOP_PRODUCTS_OF_SEAL_CHANGE_FIRM_BY_CLICKS_AMOUNT) -> list:
        """
        Retrieve the top N products by clicks for a given retailer and seal date.

        Parameters:
        haendler_bez (str): The retailer name (haendler_bez) to filter by.
        seal_date_str (str): The seal date string in the format defined by CONFIG.SEAL_CHANGE_DATE_PATTERN.
        top_n (int, optional): The number of top products to return. Defaults to CONFIG.TOP_PRODUCTS_OF_SEAL_CHANGE_FIRM_BY_CLICKS_AMOUNT.

        Returns:
        list: A list of product IDs sorted by the number of clicks.
        """
        # Convert the seal date string into a datetime object
        seal_date = dt.datetime.strptime(seal_date_str, CONFIG.SEAL_CHANGE_DATE_PATTERN)

        # Define the observation period (start and end)
        observation_start = seal_date - dt.timedelta(weeks=CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT)
        observation_end = seal_date + dt.timedelta(weeks=CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT)

        # Convert the observation period to Unix timestamps
        observation_start_unix = int(observation_start.timestamp())
        observation_end_unix = int(observation_end.timestamp())

        # Fetch the top products by clicks from the repository
        result = self.repository.fetch_top_products_by_clicks(
            haendler_bez, observation_start_unix, observation_end_unix, top_n
        ).to_series(0).to_list()

        return result
