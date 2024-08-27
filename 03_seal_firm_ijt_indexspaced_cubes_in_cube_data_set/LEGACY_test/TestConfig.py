import unittest
from datetime import datetime, timedelta

from CONFIG import UNIX_TIME_ORIGIN, UNIX_WEDNESDAY_MIDDAY_INTERCEPT, UNIX_TIME_ORIGIN_FIRST_WEEK_WEDNESDAY


class TestConfig(unittest.TestCase):

    def test_unix_time_origin_first_week_wednesday(self):
        # Calculate the expected UNIX_TIME_ORIGIN_FIRST_WEEK_WEDNESDAY
        expected_first_week_wednesday = UNIX_TIME_ORIGIN + UNIX_WEDNESDAY_MIDDAY_INTERCEPT

        # Check the value is as expected
        self.assertEqual(UNIX_TIME_ORIGIN_FIRST_WEEK_WEDNESDAY, expected_first_week_wednesday)

        # Verify the calculated timestamp corresponds to Wednesday 12 midday in GMT+2
        dt = datetime.utcfromtimestamp(UNIX_TIME_ORIGIN_FIRST_WEEK_WEDNESDAY) + timedelta(hours=2)

        self.assertEqual(dt.weekday(), 2)
        self.assertEqual(dt.hour, 12)
        self.assertEqual(dt.minute, 0)
        self.assertEqual(dt.second, 0)


if __name__ == '__main__':
    unittest.main()
