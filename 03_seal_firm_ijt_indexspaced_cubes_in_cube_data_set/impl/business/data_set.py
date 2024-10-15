import polars as pl

import CONFIG


class DataSet:
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

    def append_data(
            self,
            new_data: pl.DataFrame):

        if not self.data.is_empty():
            # if not empty for vertical stacking (default in concat) the new data has to have the same height
            if self.data.height == new_data.height:
                self.data = pl.concat([self.data, new_data])
            else:
                raise Exception("DataSet#append_data self.data.height == new_data.height has to be fulfilled.")
