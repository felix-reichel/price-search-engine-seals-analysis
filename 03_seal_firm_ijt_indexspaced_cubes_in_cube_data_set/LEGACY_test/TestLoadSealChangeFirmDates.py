import unittest
import polars as pl

from impl.static import select_seal_change_firms, load_data


class TestLoadSealChangeFirmDates(unittest.TestCase):
    def setUp(self):
        self.seal_change_firms, _, _, _ = load_data()
        self.filtered_seal_change_firms = select_seal_change_firms(self.seal_change_firms)

    def test_seal_dates_count(self):
        num_rows = self.filtered_seal_change_firms.shape[0]
        seal_dates = self.filtered_seal_change_firms.select(pl.col('Guetesiegel First Date'))
        num_seal_dates = seal_dates.shape[0]
        self.assertEqual(num_rows, num_seal_dates,
                         "The number of seal dates should be the number of rows without header.")
        self.assertEqual(num_rows, 296)


if __name__ == '__main__':
    unittest.main()
