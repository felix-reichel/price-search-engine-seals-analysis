import logging
import datetime as dt
import os
from functools import lru_cache
from dateutil.relativedelta import relativedelta
from CONFIG import UNIX_TIME_ORIGIN, UNIX_WEEK, PARQUET_FILES_DIR, ANGEBOTE_SCHEME, CLICKS_SCHEME, \
    OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED, \
    OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED

logger = logging.getLogger(__name__)


@lru_cache(maxsize=None)
def calculate_running_var_t_from_u(unix_time, unix_origin=UNIX_TIME_ORIGIN, unix_week=UNIX_WEEK):
    """
    Calculate the running variable (week number) from a Unix timestamp.
    Parameters:
    - unix_time (int/str): The Unix time or date string to calculate the running variable from.
    - unix_origin (int): The Unix time origin to base the calculation on.
    - unix_week (int): The length of a week in Unix time (seconds).
    """
    if isinstance(unix_time, str):
        unix_time = date_to_unix_time(unix_time)
    return int((unix_time - unix_origin) / unix_week)


@lru_cache(maxsize=None)
def date_to_unix_time(date_str, date_format='%d.%m.%Y'):
    """
    Convert a date string to a Unix timestamp.
    Parameters:
    - date_str (str): The date string to convert.
    - date_format (str): The format of the input date string. Default is '%d.%m.%Y'.
    """
    try:
        date_obj = dt.datetime.strptime(date_str, date_format)
        return int(date_obj.timestamp())
    except ValueError as e:
        logger.error(f"Error parsing date: {e}")
        return None


@lru_cache(maxsize=None)
def get_start_of_week(date, week_start_day=0):
    """
    Get the start of the week for a given date.
    Parameters:
    - date (datetime): The date to calculate the start of the week from.
    - week_start_day (int): The day to start the week on (0 for Monday). Default is 0.
    """
    start_of_week = date - dt.timedelta(days=(date.weekday() - week_start_day) % 7)
    return start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)


def get_unix_offer_data_inflow_time_range_from_seal_date(
        seal_date_str: str):
    seal_date = date_to_unix_time(seal_date_str)
    seal_date_dt = dt.datetime.fromtimestamp(seal_date)

    offer_time_spells_from = seal_date_dt - dt.timedelta(
        weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED)
    offer_time_spells_to = seal_date_dt + dt.timedelta(
        weeks=OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED)

    return int(offer_time_spells_from.timestamp()), int(offer_time_spells_to.timestamp())


def generate_weeks_range(start_year, end_year, scheme=ANGEBOTE_SCHEME, weeks_in_year=52, week_format="%02d"):
    """
    Generate a list of formatted week-based file names for the given range of years.
    Parameters:
    - start_year (int): The start year for the range.
    - end_year (int): The end year for the range.
    - scheme (str): The file name scheme for formatting.
    - weeks_in_year (int): Number of weeks in a year. Default is 52.
    - week_format (str): The format for the week number. Default is "%02d".
    """
    return [
        scheme.format(year=year, week=week_format % week)
        for year in range(start_year, end_year + 1)
        for week in range(1, weeks_in_year + 1)
    ]


def generate_months_range(start_year, end_year, scheme=CLICKS_SCHEME, month_format="%02d"):
    """
    Generate a list of formatted month-based file names for the given range of years.
    Parameters:
    - start_year (int): The start year for the range.
    - end_year (int): The end year for the range.
    - scheme (str): The file name scheme for formatting.
    - month_format (str): The format for the month number. Default is "%02d".
    """
    return [
        scheme.format(year=year, month=month_format % month)
        for year in range(start_year, end_year + 1)
        for month in range(1, 13)
    ]


def file_exists_in_folders(file_name, folders, base_dir=PARQUET_FILES_DIR):
    """
    Check if a file exists in any of the provided folders.
    Parameters:
    - file_name (str): The name of the file to check.
    - folders (list): A list of folder paths to search in.
    - base_dir (str): The base directory where folders are located.
    """
    if isinstance(folders, str): # FOLDERS? Only for the weak!!
        folder = folders
        file_path = os.path.join(base_dir, folder, file_name)
        if os.path.isfile(file_path):
            return file_path
    for folder in folders:
        file_path = os.path.join(base_dir, folder, file_name)
        if os.path.isfile(file_path):
            return file_path
    return None


def get_week_year_from_seal_date(seal_date, date_format="%d.%m.%Y"):
    """
    Get the ISO week number and year from a given seal date string.
    Parameters:
    - seal_date (str): The date string to convert to a week number and year.
    - date_format (str): The format of the seal date string. Default is '%d.%m.%Y'.
    """
    date_obj = dt.datetime.strptime(seal_date, date_format)
    year, week, _ = date_obj.isocalendar()
    return year, week


def get_year_month_from_seal_date(seal_date, date_format="%d.%m.%Y"):
    """
    Get the year and month from a given seal date string.
    Parameters:
    - seal_date (str): The date string to convert to a year and month.
    - date_format (str): The format of the seal date string. Default is '%d.%m.%Y'.
    """
    date_obj = dt.datetime.strptime(seal_date, date_format)
    return date_obj.year, date_obj.month


def generate_weeks_around_seal(seal_year, seal_week, pre_seal_weeks, post_seal_weeks, scheme=ANGEBOTE_SCHEME,
                               weeks_in_year=52, week_format="%02d"):
    """
    Generate a list of weeks around the seal date, considering the pre- and post-seal period.
    Parameters:
    - seal_year (int): The year of the seal date.
    - seal_week (int): The week of the seal date.
    - pre_seal_weeks (int): Number of weeks before the seal week to consider.
    - post_seal_weeks (int): Number of weeks after the seal week to consider.
    - scheme (str): The file name scheme for formatting.
    - weeks_in_year (int): Number of weeks in a year. Default is 52.
    - week_format (str): The format for the week number. Default is "%02d".
    """
    weeks_range = []
    start_week = seal_week - pre_seal_weeks
    end_week = seal_week + post_seal_weeks

    # Handle weeks before the start of the year
    for i in range(start_week, seal_week):
        year, week = _calculate_week_year(seal_year, i, weeks_in_year)
        weeks_range.append(scheme.format(year=year, week=week_format % week))

    # Handle weeks after the seal week
    for i in range(seal_week, end_week + 1):
        year, week = _calculate_week_year(seal_year, i, weeks_in_year)
        weeks_range.append(scheme.format(year=year, week=week_format % week))

    return weeks_range


def _calculate_week_year(seal_year, week, weeks_in_year):
    """
    Helper function to calculate the correct year and week based on the week range.
    Parameters:
    - seal_year (int): The year of the seal date.
    - week (int): The current week being calculated.
    - weeks_in_year (int): The number of weeks in a year.
    """
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
    """
    Generate a list of months around the seal date, considering the pre- and post-seal period.
    Parameters:
    - seal_year (int): The year of the seal date.
    - seal_month (int): The month of the seal date.
    - pre_seal_months (int): Number of months before the seal month to consider. Default is 6.
    - post_seal_months (int): Number of months after the seal month to consider. Default is 6.
    - scheme (str): The file name scheme for formatting.
    - month_format (str): The format for the month number. Default is "%02d".
    """
    months_range = []
    for i in range(-pre_seal_months, post_seal_months + 1):
        new_date = dt.datetime(seal_year, seal_month, 1) + relativedelta(months=i)
        months_range.append(scheme.format(year=new_date.year, month=month_format % new_date.month))
    return months_range
