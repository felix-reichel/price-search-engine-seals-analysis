from pathlib import Path
import pandas as pd
from CONFIG import *
from static import FileHelper


class RetailerFiltering:
    def __init__(self, allow_skip_step=True,
                 geizhals_retailer_parquet_file_path=GEIZHALS_RETAILERS_PARQUE_FILE_PATH,
                 filtered_retailer_csv_file_path=FILTERED_RETAILERS_CSV_FILE_PATH):
        self.allow_skip_step = allow_skip_step
        self.geizhals_retailer_parquet_file_path = geizhals_retailer_parquet_file_path
        self.filtered_retailer_csv_file_path = filtered_retailer_csv_file_path

    @staticmethod
    def filter_retailers(geizhals_retailer_names, forbidden_keywords=FORBIDDEN_RETAILER_KEYWORDS):
        filtered_values = [
            retailer_name for retailer_name in geizhals_retailer_names
            if all(keyword not in retailer_name for keyword in forbidden_keywords)
        ]
        num_filtered_out = len(geizhals_retailer_names) - len(filtered_values)
        print(f"{num_filtered_out} retailers filtered out in the filtering step.")
        return filtered_values

    def create_filtered_retailers_file(self):
        output_path = Path(self.filtered_retailer_csv_file_path)
        if not output_path.exists():
            try:
                retailer_df = FileHelper.read_parquet(self.geizhals_retailer_parquet_file_path)
                unique_retailers = retailer_df["haendler_bez"].unique()
                filtered_retailers = RetailerFiltering.filter_retailers(unique_retailers)
                pd.DataFrame(filtered_retailers).to_csv(self.filtered_retailer_csv_file_path,
                                                        sep=CSV_SEPARATOR)
            except Exception as e:
                print(f"Error occurred while creating filtered retailers file: {e}")
