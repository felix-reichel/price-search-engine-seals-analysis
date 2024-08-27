import unittest

import pytest
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tempfile import NamedTemporaryFile

from impl.calc.VariableCalculator import VariableCalculator, \
    mean_computation, sum_computation


@pytest.fixture
def sample_parquet_files():
    data = {
        'index1': [1, 1, 2, 2],
        'index2': [10, 20, 10, 20],
        'value': [100, 200, 300, 400]
    }
    df = pd.DataFrame(data)
    parquet_file = NamedTemporaryFile(delete=False, suffix=".parquet")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_file.name)
    yield [parquet_file.name]
    parquet_file.close()


def test_calculator_multi_index(sample_parquet_files):
    index_columns = ['index1', 'index2']

    variables = {
        "mean_var": mean_computation
    }

    calculator = VariableCalculator(sample_parquet_files, index_columns)
    results = calculator.calculate_variables(variables)

    expected_results = {
        "mean_var": {
            (1, 10): 100.0,
            (1, 20): 200.0,
            (2, 10): 300.0,
            (2, 20): 400.0
        }
    }

    assert results == expected_results


def test_calculator_single_index(sample_parquet_files):
    index_columns = ['index1']

    variables = {
        "mean_var": mean_computation
    }

    calculator = VariableCalculator(sample_parquet_files, index_columns)
    results = calculator.calculate_variables(variables)

    expected_results = {
        "mean_var": {
            (1,): 150.0,
            (2,): 350.0
        }
    }

    assert results == expected_results


def test_calculator_single_variable_computation(sample_parquet_files):
    index_columns = ['index2']

    variables = {
        "sum_var": sum_computation
    }

    calculator = VariableCalculator(sample_parquet_files, index_columns)
    results = calculator.calculate_variables(variables)

    expected_results = {
        "sum_var": {
            (10,): 400,
            (20,): 600
        }
    }

    assert results == expected_results


if __name__ == '__main__':
    unittest.main()
