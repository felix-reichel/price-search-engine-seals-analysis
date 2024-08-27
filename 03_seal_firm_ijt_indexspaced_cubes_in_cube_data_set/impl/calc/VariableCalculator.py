import pandas as pd
import pyarrow.parquet as pq
from multiprocessing import Pool, cpu_count
from tqdm import tqdm


def mean_computation(data):
    return data['value'].mean()


def sum_computation(data):
    return data['value'].sum()


def _compute_variable(data, variable_func):
    return variable_func(data)


class VariableCalculator:
    def __init__(self, parquet_files, index_columns):
        self.parquet_files = parquet_files
        self.index_columns = index_columns

    def _extract_unique_combinations(self, df):
        return df[self.index_columns].drop_duplicates()

    def _load_data_from_parquet(self, index_values):
        filtered_data = []
        for file in self.parquet_files:
            table = pq.read_table(file)
            df = table.to_pandas()
            filtered_df = df
            for idx_col, idx_val in zip(self.index_columns, index_values):
                filtered_df = filtered_df[filtered_df[idx_col] == idx_val]
            filtered_data.append(filtered_df)
        return pd.concat(filtered_data, ignore_index=True)

    def _process_variable_for_combination(self, index_combination, variable_func):
        data = self._load_data_from_parquet(index_combination)
        computed_value = _compute_variable(data, variable_func)
        return tuple(index_combination), computed_value  # Convert to tuple for hash-ability

    def calculate_variables(self, variables):
        results = {}
        df = pd.concat([pd.read_parquet(file) for file in self.parquet_files])
        unique_combinations = self._extract_unique_combinations(df)

        for variable_name, variable_func in variables.items():
            unique_combinations_list = unique_combinations.apply(tuple, axis=1).tolist()  # Convert rows to tuples

            # Use tqdm to show the progress bar
            with Pool(cpu_count()) as pool:
                computed_results = list(tqdm(
                    pool.starmap(
                        self._process_variable_for_combination,
                        [(comb, variable_func) for comb in unique_combinations_list]
                    ),
                    desc=f"Calculating {variable_name}",
                    total=len(unique_combinations_list)
                ))
                results[variable_name] = dict(computed_results)  # Convert results to dictionary
        return results


# Example usage
if __name__ == "__main__":
    parquet_files = ["data1.parquet", "data2.parquet"]
    index_columns = ["index1", "index2"]

    variables = {
        "mean_var": mean_computation,
        "sum_var": sum_computation
    }

    calculator = VariableCalculator(parquet_files, index_columns)
    results = calculator.calculate_variables(variables)
    print(results)

    # TODO: Fill out DataFrame variable columns for affected rows for variable X_{index_columns}.
