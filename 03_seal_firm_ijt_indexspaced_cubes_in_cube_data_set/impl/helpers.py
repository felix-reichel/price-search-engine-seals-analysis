import datetime as dt
import logging
import os
from functools import lru_cache

import psutil
from dateutil.relativedelta import relativedelta

from CONFIG import UNIX_TIME_ORIGIN, UNIX_WEEK, PARQUET_FILES_DIR, ANGEBOTE_SCHEME, CLICKS_SCHEME, \
    OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED, \
    OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED

logger = logging.getLogger(__name__)


def print_process_mem_usage():
    process = psutil.Process()
    mem_info = process.memory_info()
    print(f"Memory Usage: {mem_info.rss / (1024 * 1024):.2f} MB")


@lru_cache(maxsize=None)
def calculate_running_var_t_from_u(unix_time, unix_origin=UNIX_TIME_ORIGIN, unix_week=UNIX_WEEK):
    if isinstance(unix_time, str):
        unix_time = date_to_unix_time(unix_time)
    return int((unix_time - unix_origin) / unix_week)


@lru_cache(maxsize=None)
def calculate_unix_time_from_running_var(week_running_var, unix_origin=UNIX_TIME_ORIGIN, unix_week=UNIX_WEEK):
    """
    Calculate the unix timestamp from a given week_running_var (reverse of calculate_running_var_t_from_u).
    """
    if isinstance(week_running_var, int):
        return unix_origin + (week_running_var * unix_week)
    else:
        raise NotImplementedError("week_running_var supplied in helpers#calculate_unix_time_from_running_var has be "
                                  "of type Int.")


@lru_cache(maxsize=None)
def date_to_unix_time(date_str, date_format='%d.%m.%Y'):
    try:
        date_obj = dt.datetime.strptime(date_str, date_format)
        return int(date_obj.timestamp())
    except ValueError as e:
        logger.error(f"Error parsing date: {e}")
        return None


@lru_cache(maxsize=None)
def get_start_of_week(date, week_start_day=0):
    start_of_week = date - dt.timedelta(days=(date.weekday() - week_start_day) % 7)
    return start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)


@lru_cache(maxsize=None)
def get_unix_offer_data_inflow_time_range_from_seal_date(seal_date_str: str):
    seal_date = date_to_unix_time(seal_date_str)
    seal_date_dt = dt.datetime.fromtimestamp(seal_date)

    offer_time_spells_from = seal_date_dt - dt.timedelta(
        weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED)
    offer_time_spells_to = seal_date_dt + dt.timedelta(
        weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED)

    return int(offer_time_spells_from.timestamp()), int(offer_time_spells_to.timestamp())


@lru_cache(maxsize=None)
def get_week_year_from_seal_date(seal_date, date_format="%d.%m.%Y"):
    date_obj = dt.datetime.strptime(seal_date, date_format)
    year, week, _ = date_obj.isocalendar()
    return year, week


@lru_cache(maxsize=None)
def get_year_month_from_seal_date(seal_date, date_format="%d.%m.%Y"):
    date_obj = dt.datetime.strptime(seal_date, date_format)
    return date_obj.year, date_obj.month


def file_exists_in_folders(file_name, folders, base_dir=PARQUET_FILES_DIR):
    if isinstance(folders, str):
        folder = folders
        file_path = os.path.join(base_dir, folder, file_name)
        if os.path.isfile(file_path):
            return file_path
    for folder in folders:
        file_path = os.path.join(base_dir, folder, file_name)
        if os.path.isfile(file_path):
            return file_path
    return None


def generate_weeks_range(start_year, end_year, scheme=ANGEBOTE_SCHEME, weeks_in_year=52, week_format="%02d"):
    return [
        scheme.format(year=year, week=week_format % week)
        for year in range(start_year, end_year + 1)
        for week in range(1, weeks_in_year + 1)
    ]


def generate_months_range(start_year, end_year, scheme=CLICKS_SCHEME, month_format="%02d"):
    return [
        scheme.format(year=year, month=month_format % month)
        for year in range(start_year, end_year + 1)
        for month in range(1, 13)
    ]


def generate_weeks_around_seal(seal_year, seal_week, pre_seal_weeks, post_seal_weeks, scheme=ANGEBOTE_SCHEME,
                               weeks_in_year=52, week_format="%02d"):
    weeks_range = []
    start_week = seal_week - pre_seal_weeks
    end_week = seal_week + post_seal_weeks

    for i in range(start_week, seal_week):
        year, week = _calculate_week_year(seal_year, i, weeks_in_year)
        weeks_range.append(scheme.format(year=year, week=week_format % week))

    for i in range(seal_week, end_week + 1):
        year, week = _calculate_week_year(seal_year, i, weeks_in_year)
        weeks_range.append(scheme.format(year=year, week=week_format % week))

    return weeks_range


@lru_cache(maxsize=None)
def _calculate_week_year(seal_year, week, weeks_in_year):
    if week <= 0:
        year = seal_year - 1
        week = weeks_in_year + week
    elif week > weeks_in_year:
        year = seal_year + 1
        week = week - weeks_in_year
    else:
        year = seal_year
    return year, week


def generate_months_around_seal(seal_year, seal_month, pre_seal_months=6, post_seal_months=6, scheme=CLICKS_SCHEME,
                                month_format="%02d"):
    months_range = []
    for i in range(-pre_seal_months, post_seal_months + 1):
        new_date = dt.datetime(seal_year, seal_month, 1) + relativedelta(months=i)
        months_range.append(scheme.format(year=new_date.year, month=month_format % new_date.month))
    return months_range
