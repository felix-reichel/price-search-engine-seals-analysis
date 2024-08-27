from pathlib import Path
import pandas as pd
from CONFIG import *


class FileHelper:
    @staticmethod
    def read_parquet(file_path):
        """Reads data from a Parquet file."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"Parquet file '{file_path}' not found.")
        return pd.read_parquet(file_path)

    @staticmethod
    def read_csv(file_path):
        """Reads data from a CSV file."""
        path = Path(file_path)
        if not path.exists():
            raise FileNotFoundError(f"CSV file '{file_path}' not found.")
        return pd.read_csv(file_path, sep=CSV_SEPARATOR, header=None)
