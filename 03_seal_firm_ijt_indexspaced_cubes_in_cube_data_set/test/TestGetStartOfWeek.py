import datetime as dt
import unittest

from CONFIG import UNIX_DAY, UNIX_WEDNESDAY_MIDDAY_INTERCEPT
from impl.static import get_start_of_week


class TestGetStartOfWeek(unittest.TestCase):

    def test_get_start_of_week(self):
        # sample date (Thursday midday)
        thursday_midday = dt.datetime(2024, 8, 15, 12, 0, 0)

        # Call the method
        start_of_week = get_start_of_week(thursday_midday)

        # Calculate the expected start of the week (Monday at 00:00:00)
        expected_start_of_week = thursday_midday - dt.timedelta(days=3.5)
        expected_start_of_week = expected_start_of_week.replace(hour=0, minute=0, second=0, microsecond=0)

        # Assert the expected start of the week
        self.assertEqual(start_of_week, expected_start_of_week)

        # Additional check for UNIX_WEDNESDAY_MIDDAY_INTERCEPT time constants from CONFIG
        expected_start_of_week_unix = thursday_midday.timestamp() - UNIX_WEDNESDAY_MIDDAY_INTERCEPT - UNIX_DAY
        self.assertEqual(start_of_week.timestamp(), expected_start_of_week_unix)


if __name__ == '__main__':
    unittest.main()
