from typing import List

import polars as pl


class DataSet:
    def __init__(self, data: pl.DataFrame, db_source):
        """
        DataSet abstraction that handles data management, loading, querying, and exporting.
        """
        self.data = data
        self.db_source = db_source

    def load_required_tables(self, table_names: List[str]):
        """
        Load required tables from the database dynamically.
        """
        for table in table_names:
            if table not in self.db_source.tables():
                self.db_source.load_table(table)

    def execute_query(self, query: str) -> pl.DataFrame:
        """
        Execute a query against the loaded data.
        """
        return self.db_source.execute_query(query)

    def export_to_csv(self, filename: str):
        """
        Export the dataset to CSV after all processing is done.
        """
        self.data.write_csv(filename)

    def append_results(self, new_data: pl.DataFrame):
        """
        Append new results (from batch processing) to the dataset.
        """
        self.data = pl.concat([self.data, new_data], how="vertical")