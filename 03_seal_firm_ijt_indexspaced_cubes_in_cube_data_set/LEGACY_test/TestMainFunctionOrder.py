import unittest
from unittest.mock import patch

import polars as pl

from impl.main import main


class TestMainFunctionOrder(unittest.TestCase):

    @patch('impl.main.process_seal_firm')
    @patch('impl.main.pl.read_parquet')
    @patch('impl.main.select_seal_change_firms')
    @patch('impl.main.load_data')
    def test_main_calls_order(self, mock_load_data, mock_select_seal_change_firms, mock_read_parquet,
                              mock_process_seal_firm):
        # Mock the return value of load_data to return DataFrames with necessary structure
        mock_load_data.return_value = (
            pl.DataFrame({
                'mock col': ['firm1', 'firm2', 'firm3'],  # Column for seal_change_firms
                'Geizhals ID': [1, 2, 3],  # Geizhals identifier, dummy data,
                'RESULTING MATCH': ['firm1', 'firm2', 'firm3'],
                'Seal Date': ['2024-01-01', '2024-02-01', '2024-03-01']  # Seal change date, dummy data
            }),
            pl.DataFrame(),  # filtered_haendler_bez
            pl.DataFrame({'produkt_id': [101, 102, 103]}),  # products_df
            pl.DataFrame()  # retailers_df
        )

        # Create a mock DataFrame with the necessary "RESULTING MATCH" column
        mock_select_seal_change_firms.return_value = pl.DataFrame({
            'mock col': ['firm1', 'firm2', 'firm3'],  # Column for seal_change_firms
            'Geizhals ID': [1, 2, 3],  # Geizhals identifier, dummy data,
            'RESULTING MATCH': ['firm1', 'firm2', 'firm3'],
            'Seal Date': ['2024-01-01', '2024-02-01', '2024-03-01']  # Seal change date, dummy data
        })

        # Mock read_parquet to return an empty DataFrame (or any appropriate structure)
        mock_read_parquet.return_value = pl.DataFrame()

        # Mock process_seal_firm to return an empty list (as if no firms were processed)
        mock_process_seal_firm.return_value = []

        # Act: Call the main function
        main(parallel=False)

        # Assert: Check that the mocked functions were called in the expected order
        mock_load_data.assert_called_once()
        mock_select_seal_change_firms.assert_called_once_with(mock_load_data.return_value[0])
        self.assertGreater(mock_read_parquet.call_count, 0)
        self.assertGreater(mock_process_seal_firm.call_count, 0)


if __name__ == '__main__':
    unittest.main()
