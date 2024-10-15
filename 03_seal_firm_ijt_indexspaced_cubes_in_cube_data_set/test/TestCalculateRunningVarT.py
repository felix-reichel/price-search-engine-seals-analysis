import unittest

from CONFIG import UNIX_TIME_ORIGIN, UNIX_WEEK
from impl.helpers import calculate_running_var_t_from_u


class TestCalculateRunningVarT(unittest.TestCase):

    def test_calculate_running_var_t_from_u(self):

        test_cases = [
            (UNIX_TIME_ORIGIN, 0.0),
            (UNIX_TIME_ORIGIN + UNIX_WEEK, 1.0),
            (UNIX_TIME_ORIGIN + 2 * UNIX_WEEK, 2.0),
            (UNIX_TIME_ORIGIN - UNIX_WEEK, -1.0),
            (UNIX_TIME_ORIGIN + 100 * UNIX_WEEK, 100.0),
        ]

        for unix_time, expected_t in test_cases:
            with self.subTest(unix_time=unix_time):
                actual_t = calculate_running_var_t_from_u(unix_time)
                self.assertAlmostEqual(actual_t, expected_t, places=5)


if __name__ == '__main__':
    unittest.main()

