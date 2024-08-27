import os
import pandas as pd
import dask
from dask import delayed
from dask import dataframe as dd

INPUT_DIRS = ['/nfn_vwl/geizhals/zieg_pq_db/angebot',
              '/nfn_vwl/geizhals/zieg_pq_db/angebot_06_10',
              '/nfn_vwl/geizhals/zieg_pq_db/angebot_11_15']

OUTPUT_DIR = 'data/output/'
OUTPUT_FILE_PATH = os.path.join(OUTPUT_DIR, 'population_counts.csv')
FILTERED_HAENDLER_BEZ_PATH = 'data/input/' + 'filtered_haendler_bez.csv'
YEARS = range(2006, 2022)  # Define the range of years to collect Angebote
USE_LEAP_WEEK = True  # Define whether to use the leap week for leap years

# Ensure the output folder exists
os.makedirs(OUTPUT_DIR, exist_ok=True)

filtered_haendler_bez_df = pd.read_csv(FILTERED_HAENDLER_BEZ_PATH, sep=";")
filtered_haendler_bez = filtered_haendler_bez_df.iloc[:, 1].tolist()  # haendler_bez is in the 2nd column

results = []


# Define a delayed function to process each year
@delayed
def process_year(year):
    parquet_files = []
    for week in range(1, 52 if USE_LEAP_WEEK and year % 4 == 0 else 53):
        for input_dir in INPUT_DIRS:
            parquet_file = os.path.join(input_dir, f'angebot_{year}w{week:02d}.parquet')
            if os.path.exists(parquet_file):
                parquet_files.append(parquet_file)

    if len(parquet_files) >= 2:
        # Take the first and last parquet files for each year
        parquet_files = [parquet_files[0], parquet_files[-1]]

        # Read Parquet files as Dask DataFrames
        df1 = dd.read_parquet(parquet_files[0])
        df2 = dd.read_parquet(parquet_files[1])

        # haendler suffering from 'panel attrition' will simply fall out
        common_haendler_bez = set(df1['haendler_bez']).intersection(set(df2['haendler_bez']))

        count = sum(1 for hb in filtered_haendler_bez if hb in common_haendler_bez)

        common_haendler = [hb for hb in filtered_haendler_bez if hb in common_haendler_bez]

        return {'year': year, 'count': count, 'common_haendler': common_haendler}
    else:
        print(f"Not enough Parquet files for year {year}")


delayed_results = [process_year(year) for year in YEARS]
computed_results = dask.compute(*delayed_results)

results.extend(computed_results)

population_df = pd.DataFrame(results)
population_df.to_csv(OUTPUT_FILE_PATH, index=False, sep=";")

# Print results
for result in results:
    print(f"Year: {result['year']}, Count: {result['count']}, Common Haendler: {result['common_haendler']}")

print("Base Population matrix has been written to:", OUTPUT_FILE_PATH)
