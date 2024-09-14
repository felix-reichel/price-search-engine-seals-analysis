import logging
import datetime as dt
import os
from functools import lru_cache

from dateutil.relativedelta import relativedelta

from CONFIG import UNIX_TIME_ORIGIN, UNIX_WEEK, ANGEBOTE_SCHEME, CLICKS_SCHEME, \
    OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED, PARQUE_FILES_DIR, \
    OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED

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


@lru_cache(maxsize=None)
def get_start_of_week(date: dt):
    start_of_week = date - dt.timedelta(days=date.weekday())
    start_of_week = start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)
    return start_of_week


def generate_weeks_range(start_year, end_year):
    return [
        ANGEBOTE_SCHEME.format(year=year, week=f"{week:02d}")
        for year in range(start_year, end_year + 1)
        for week in range(1, 54)
    ]


def generate_months_range(start_year, end_year):
    return [
        CLICKS_SCHEME.format(year=year, month=f"{month:02d}")
        for year in range(start_year, end_year + 1)
        for month in range(1, 13)
    ]


def file_exists_in_folders(file_name, folders):
    for folder in folders:
        file_path = os.path.join(PARQUE_FILES_DIR, folder, file_name)
        if os.path.isfile(file_path):
            return file_path
    return None


def get_week_year_from_seal_date(seal_date):
    date_obj = dt.datetime.strptime(seal_date, "%d.%m.%Y")
    year, week, _ = date_obj.isocalendar()
    return year, week


def get_year_month_from_seal_date(seal_date):
    date_obj = dt.datetime.strptime(seal_date, "%d.%m.%Y")
    return date_obj.year, date_obj.month


def generate_weeks_around_seal(seal_year, seal_week):
    weeks_range = []
    start_week = seal_week - OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED
    end_week = seal_week + OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED

    # Handle weeks before the start of the year
    for i in range(start_week, seal_week):
        if i <= 0:
            year = seal_year - 1
            week = 52 + i  # Assuming 52 weeks per year
        else:
            year = seal_year
            week = i
        weeks_range.append(ANGEBOTE_SCHEME.format(year=year, week=f"{week:02d}"))

    # Handle weeks after the seal week
    for i in range(seal_week, end_week + 1):
        if i > 52:
            year = seal_year + 1
            week = i - 52
        else:
            year = seal_year
            week = i
        weeks_range.append(ANGEBOTE_SCHEME.format(year=year, week=f"{week:02d}"))

    return weeks_range


def generate_months_around_seal(seal_year, seal_month):
    months_range = []
    for i in range(-6, 7):  # from 6 months before to 6 months after
        new_date = dt.datetime(seal_year, seal_month, 1) + relativedelta(months=i)
        year = new_date.year
        month = new_date.month
        months_range.append(CLICKS_SCHEME.format(year=year, month=f"{month:02d}"))
    return months_range
