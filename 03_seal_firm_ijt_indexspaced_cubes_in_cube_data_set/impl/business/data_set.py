import polars as pl

import CONFIG
from impl import EMPTY_ROW


class GZDataSet:
    def __init__(self,
                 name: str,
                 filename: str):
        self.name = name,
        self.filename = filename,
        self.data = pl.DataFrame()

    def export_to_csv(
            self,
            filename: str):

        self.data.write_csv(
            filename,
            separator=CONFIG.CSV_OUTPUT_DELIM_STYLE)

    def append_new_variable(
            self,
            new_data: pl.DataFrame):

        if not self.data.is_empty():
            # if not empty for vertical stacking (default in concat) the new data has to have the same height
            if self.data.height == new_data.height:
                self.data = pl.concat([self.data, new_data], how="vertical")
            else:
                raise Exception("DataSet#append_new_variable self.data.height == new_data.height has to be fulfilled.")

    def append_new_row(
            self,
            new_data: pl.DataFrame):

        if not self.data.is_empty():
            if self.data.width == new_data.width:
                self.data = pl.concat([self.data, new_data], how="horizontal")
            else:
                raise Exception("DataSet#append_new_row self.data.width == new_data.width has to be fulfilled.")

    def append_new_empty_row_with_existing_values_dict(self, values: dict):
        NEW_ROW = EMPTY_ROW.empty_df_polars

        for col_name, value in values.items():
            if col_name in NEW_ROW.columns:
                NEW_ROW = NEW_ROW.with_columns(
                    pl.lit(value)
                    .alias(col_name)
                )
            else:
                raise Exception(f"Column '{col_name}' not found in the dataset.")

        self.append_new_row(NEW_ROW)
