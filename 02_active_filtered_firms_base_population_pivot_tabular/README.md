# Fr-02 Geizhals Guetesiegel Geizals Retailer - Population count by year

**Author**: FR

**Date**: 6th June 2024

## Introduction

This script calculates the base population counts of `haendler_bez` (retailers) for each year. The count for a year is determined by finding the retailers that appear in two specific Parquet files for that year.

**Note**: It is recommended to read **Fr-01** before proceeding with **Fr-02** for a better understanding of the context and requirements.

## Definitions

- **haendler_bez**: GZ Retailer identifiers.
- **GZ Angebot-Parquet Files**: Columnar storage files used for efficient data processing.
- **filtered_haendler_bez.csv**: CSV file containing the list of `haendler_bez` to be counted.

## Script Step-by-Step Explanation

1. **Read the Filtered Retailers CSV File**:
    - The script reads the CSV file (`/data/input/filtered_haendler_bez.csv`) to obtain the list of retailer identifiers (`haendler_bez`) to be considered.

2. **Read the Parquet Files for Each Year**:
    - For each year, the script reads two Parquet files (`/data/input/angebot_{year}w01.parquet` and `/data/input/angebot_{year}w53.parquet`). These files contain data on retailers for the respective year.
    - Therefore, retailers suffering from (let's say "panel attrition") will fall out.

3. **Count Occurrences**:
    - The script finds common `haendler_bez` that appear in both Parquet files for a specific year.
    - It then counts how many of these common `haendler_bez` are present in the list from the CSV file.

4. **Output the Results**:
    - The script outputs the counts of `haendler_bez` for each year.

## Usage

1. Place the script (`population_count.py`) in the same directory as the input data (`/data/input`).
2. Ensure the directory contains the `filtered_haendler_bez.csv` and the necessary Parquet files (`angebot_{year}w01.parquet` and `angebot_{year}w53.parquet` for each year).
3. Run the script using Python:
    ```bash
    python population_count.py
    ```
4. The script will print the population count for each year.
