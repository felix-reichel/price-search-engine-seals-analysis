import os
import time
import dask.dataframe as dd
import polars as pl
from CONFIG import PARQUE_FILES_DIR, ANGEBOTE_FOLDER


def get_file_paths(folder):
    folder_path = PARQUE_FILES_DIR / folder
    return [str(folder_path / f) for f in os.listdir(folder_path) if f.endswith(".parquet")]


def calculate_average(times):
    return sum(times) / len(times)


def test_performance_dask_vs_polars():
    angebot_file_paths = []
    for folder in ANGEBOTE_FOLDER:
        files = get_file_paths(folder)
        angebot_file_paths.extend(files)

    assert len(angebot_file_paths) > 0, "No files found in the Angebots folders"

    dask_read_times = []
    dask_filter_times = []
    polars_read_times = []
    polars_filter_times = []

    repetitions = 10

    for i in range(repetitions):
        print(f"Starting Dask iteration {i + 1}")

        start_time_dask = time.time()
        dask_df = dd.read_parquet(angebot_file_paths)
        dask_read_times.append(time.time() - start_time_dask)

        start_time_filter_dask = time.time()
        filtered_dask_df = dask_df[dask_df['preis_min'] > 100]
        filtered_dask_df.compute()  # Trigger the actual computation
        dask_filter_times.append(time.time() - start_time_filter_dask)

    for i in range(repetitions):
        print(f"Starting Polars iteration {i + 1}")

        start_time_polars = time.time()
        polars_df = pl.read_parquet(angebot_file_paths)
        polars_read_times.append(time.time() - start_time_polars)

        start_time_filter_polars = time.time()
        filtered_polars_df = polars_df.filter(pl.col('preis_min') > 100)
        polars_filter_times.append(time.time() - start_time_filter_polars)

    avg_dask_read_time = calculate_average(dask_read_times)
    avg_dask_filter_time = calculate_average(dask_filter_times)
    avg_polars_read_time = calculate_average(polars_read_times)
    avg_polars_filter_time = calculate_average(polars_filter_times)

    print(f"\nPerformance comparison (averaged over {repetitions} runs):")
    print(f"Average Dask read time: {avg_dask_read_time:.2f} seconds")
    print(f"Average Dask filter time: {avg_dask_filter_time:.2f} seconds")
    print(f"Average Polars read time: {avg_polars_read_time:.2f} seconds")
    print(f"Average Polars filter time: {avg_polars_filter_time:.2f} seconds")

    # Performance comparison (averaged over 10 runs):
    # Average Dask read time: 0.01 seconds
    # Average Dask filter time: 1.15 seconds
    # Average Polars read time: 0.20 seconds
    # Average Polars filter time: 0.02 seconds


if __name__ == "__main__":
    test_performance_dask_vs_polars()
