import datetime as dt
import unittest

import polars as pl

from impl.static import get_offered_weeks, calculate_running_var_t_from_u


@DeprecationWarning
class TestGetOfferedWeeks(unittest.TestCase):

    def test_get_offered_weeks_within_observation_window(self):
        # Sample angebot_data
        angebot_data = pl.DataFrame({
            'produkt_id': ['P1', 'P1', 'P2', 'P1'],
            'haendler_bez': ['F1', 'F1', 'F2', 'F1'],
            'dtimebegin': [
                int(dt.datetime(2022, 12, 25).timestamp()),
                int(dt.datetime(2023, 8, 15).timestamp()),
                int(dt.datetime(2021, 8, 14).timestamp()),
                int(dt.datetime(2024, 1, 1).timestamp())
            ],
            'dtimeend': [
                int(dt.datetime(2023, 1, 2).timestamp()),
                int(dt.datetime(2023, 8, 18).timestamp()),
                int(dt.datetime(2021, 8, 15).timestamp()),
                int(dt.datetime(2024, 1, 7).timestamp())
            ]
        })

        # test parameters
        prod_id = 'P1'
        firm_id = 'F1'
        seal_date = "15.8.2023"  # int(dt.datetime(2023, 8, 15).timestamp())

        expected_weeks = {  # only max -26 weeks before to +26 weeks after the seal change...
            int(calculate_running_var_t_from_u(int(dt.datetime(2023, 8, 17).timestamp()))),
            int(calculate_running_var_t_from_u(int(dt.datetime(2024, 1, 7).timestamp()))),
        }

        # expected_weeks = {
        #    int(calculate_running_var_t_from_u(int(dt.datetime(2022, 12, 25).timestamp()))),
        #    int(calculate_running_var_t_from_u(int(dt.datetime(2023, 1, 2).timestamp()))),
        #    int(calculate_running_var_t_from_u(int(dt.datetime(2023, 8, 17).timestamp()))),
        #    int(calculate_running_var_t_from_u(int(dt.datetime(2024, 1, 7).timestamp()))),
        #    815  # seems legit
        # }

        result_weeks = get_offered_weeks(angebot_data, prod_id, firm_id, seal_date)
        self.assertEqual(expected_weeks, result_weeks)


if __name__ == '__main__':
    unittest.main()
