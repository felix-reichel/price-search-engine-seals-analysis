import datetime as dt
import logging

import CONFIG
from impl.repository import ClicksRepository

logger = logging.getLogger(__name__)


class ClicksService:
    def __init__(
            self,
            repository: ClicksRepository):

        self.repository = repository

    def get_top_n_products_by_clicks(
            self,
            haendler_bez: str,
            seal_date_str: str,
            top_n: int = CONFIG.TOP_PRODUCTS_OF_SEAL_CHANGE_FIRM_BY_CLICKS_AMOUNT):

        seal_date = dt.datetime.strptime(seal_date_str,
                                         CONFIG.SEAL_CHANGE_DATE_PATTERN)

        observation_start = seal_date - dt.timedelta(
            weeks=CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT)
        observation_end = seal_date + dt.timedelta(
            weeks=CONFIG.HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT)

        observation_start_unix = int(observation_start.timestamp())
        observation_end_unix = int(observation_end.timestamp())

        result = self.repository.fetch_top_products_by_clicks(
            haendler_bez, observation_start_unix, observation_end_unix, top_n
        )
        return result['produkt_id'].to_list()
