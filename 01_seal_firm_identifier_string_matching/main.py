# main.py

"""
 <insert readme.md here>
"""
import os
from pathlib import Path

import pandas as pd

from CONFIG import *
from src.Matching import ECommerceMatching, EhiBevhMatching, HandelsverbandMatching
from src.Plotting import Plotting
from src.RetailerFiltering import RetailerFiltering
from static import FileHelper, StringHelper


class RetailerProcessor:
    def __init__(self, allow_skip_step=ALLOW_SKIPPING):
        self.allow_skip_step = allow_skip_step

    def filter_retailers(self):
        retailer_filtering = RetailerFiltering(self.allow_skip_step)
        if retailer_filtering.allow_skip_step and Path(FILTERED_RETAILERS_CSV_FILE_PATH).exists():
            print(f"Skipping filter_retailers step 0 as {FILTERED_RETAILERS_CSV_FILE_PATH} already exists.")
            return
        retailer_filtering.create_filtered_retailers_file()

    def match_retailers(self):
        output_files = [Path(file) for file in (ECOMMERCE_MATCHED_FILE_PATH, EHI_BEVH_MATCHED_FILE_PATH,
                                                HANDELSVERBAND_MATCHED_FILE_PATH)]
        if self.allow_skip_step and all(file.exists() for file in output_files):
            print(f"Skipping match_retailers step 1 as all matched files already exist.")
            return
        matched_retailers_set = set()
        filtered_retailers_df = FileHelper.read_csv(FILTERED_RETAILERS_CSV_FILE_PATH)
        total_retailers = len(filtered_retailers_df)

        ecommerce_matching = ECommerceMatching(filtered_retailers_df, matched_retailers_set)
        ecommerce_matching.run_matching()

        ehi_bevh_matching = EhiBevhMatching(filtered_retailers_df, matched_retailers_set)
        ehi_bevh_matching.run_matching()

        handelsverband_matching = HandelsverbandMatching(filtered_retailers_df, matched_retailers_set)
        handelsverband_matching.run_matching()

        print(f"=== SUMMARY ===\nAll Unique retailer matching candidates: {len(matched_retailers_set)} "
              f"out of {total_retailers} retailers")
        Plotting.plot_nchars_distribution(filtered_retailers_df)

    def post_matching_processing(self):
        # Implement the logic for post-matching processing
        print(f"Nothing todo in post_matching_processing step 2 ")
        # Manual Review: Retailers with lower probability matches are reviewed and matched manually
        # in a column ("RESULTING MATCH").
        # Datasets (with column("RESULTING MATCH") after manual Review):
        # input / e_commerce_reviewed.csv
        # input / ehi_bevh_reviewed.csv
        # input / handelsverband_reviewed.csv

    def create_overview(self):
        final_output_path = Path(FINAL_MATRIX_FILE_PATH)
        if self.allow_skip_step and final_output_path.exists():
            print(f"Skipping create_overview step 3 as {final_output_path} already exists.")
            return

        haendler_df = pd.read_parquet(GEIZHALS_RETAILERS_PARQUE_FILE_PATH)

        reviewed_files_info = [
            (ECOMMERCE_GUETESIEGEL_REVIEWED_FILE_PATH, E_COMMERCE_DATE_FROM_COL_NAME),
            (EHI_BEVH_GUETESIEGEL_REVIEWED_FILE_PATH, EHI_BHV_DATE_FROM_COL_NAME),
            (HANDELSVERBAND_GUETESIEGEL_REVIEWED_FILE_PATH, HANDELSVERBAND_DATE_FROM_COL_NAME)
        ]

        reviewed_dfs = []

        for file_path, matrice_col_name in reviewed_files_info:
            if os.path.exists(file_path):
                df = pd.read_csv(file_path, sep=CSV_SEPARATOR)
                df_filtered = df[(df[HEADER_COLUMN_RESULTING_MATCH_REVIEWED_FRANZ_WRONG_COL_NAME].notna())
                                 & (df[HEADER_COLUMN_RESULTING_MATCH_REVIEWED_FRANZ_WRONG_COL_NAME] !=
                                    EXCLUDE_MATCH_NO_CANDIDATE_FRANZ)].copy()

                df_filtered.loc[:, 'Resulting matrice column merged on col names'] = df_filtered[matrice_col_name]
                df_filtered.loc[:, 'Guetesiegel_bez'] = df_filtered.iloc[:, 1]  # 2nd column
                df_filtered.loc[:, 'Filename'] = os.path.basename(file_path)  # adds from file context
                reviewed_dfs.append(df_filtered)

        all_reviewed_df = pd.concat(reviewed_dfs, ignore_index=True)

        merged_df = pd.merge(
            all_reviewed_df,
            haendler_df,
            how='left',
            left_on=HEADER_COLUMN_RESULTING_MATCH_REVIEWED_FRANZ_WRONG_COL_NAME,
            right_on='haendler_bez'
        )

        # merged_df = merged_df.drop_duplicates(subset=HEADER_COLUMN_RESULTING_MATCH,
        #                                      keep='last')

        final_df = pd.DataFrame({
            'Filename': merged_df['Filename'],
            'FilenameCp': merged_df['Filename'],
            'Guetesiegel_bez': merged_df['Guetesiegel_bez'],
            'RESULTING MATCH': merged_df[HEADER_COLUMN_RESULTING_MATCH_REVIEWED_FRANZ_WRONG_COL_NAME],
            'Guetesiegel First Date': merged_df['Resulting matrice column merged on col names'],
            'is_at': merged_df['is_at'].fillna(0).astype(int),
            'is_de': merged_df['is_de'].fillna(0).astype(int),
            'is_not_de_and_not_at': (~(merged_df['is_de']) & ~(merged_df['is_at'])).fillna(1).astype(int)
        })

        # Replace -2 with 0
        final_df['is_not_de_and_not_at'] = final_df['is_not_de_and_not_at'].replace(-2, 0)

        def drop_duplicates_within_group(group):
            return group.drop_duplicates(
                subset='RESULTING MATCH',
                keep='last')

        final_df = (final_df.groupby('Filename', as_index=True)
                    .apply(drop_duplicates_within_group, include_groups=False)
                    .reset_index(drop=True))

        final_df.to_csv(FINAL_MATRIX_FILE_PATH, sep=CSV_SEPARATOR, index=False)

        # for filename, group_df in final_df.groupby('Filename', as_index=False):
        #    group_df = group_df.groupby('Guetesiegel_bez').apply(drop_duplicates_within_group).reset_index(drop=True)
        #    output_file_path = os.path.join(OUTPUT_FOLDER, f"{filename}_merged.csv")
        #    group_df.to_csv(output_file_path, sep=CSV_SEPARATOR, index=False)

    def create_counts_stats(self):
        final_matrice = FINAL_MATRIX_FILE_PATH
        count_matrice = COUNT_MATRIX_FILE_PATH

        if self.allow_skip_step and Path(count_matrice).exists():
            print(f"Skipping create_counts_stats step as {count_matrice} already exists.")
            return

        df = pd.read_csv(final_matrice, sep=CSV_SEPARATOR)

        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        df['year'] = df.apply(lambda row: StringHelper.extract_year(row[COLUMN_DATE]), axis=1)

        count_matrix = df.groupby([COLUMN_FILENAMECP, COLUMN_YEAR, COLUMN_IS_AT,
                                   COLUMN_IS_DE, 'is_not_de_and_not_at']).size().reset_index(name='count')

        pivot_df = count_matrix.pivot_table(
            index=[COLUMN_FILENAMECP, COLUMN_IS_AT, COLUMN_IS_DE, 'is_not_de_and_not_at'],
            columns=COLUMN_YEAR,
            values='count',
            fill_value=0,
            aggfunc='sum'
        ).reset_index()

        pivot_df.columns.name = None

        pivot_df = pivot_df.astype(
            {col: int for col in pivot_df.columns if
             col not in [COLUMN_FILENAMECP, COLUMN_IS_AT, COLUMN_IS_DE, 'is_not_de_and_not_at']}
        )

        pivot_df.to_csv(count_matrice, index=False, sep=CSV_SEPARATOR)

    def run(self):
        # Step 0 - Skipped if output exists and CONFIG.ALLOW_SKIPPING is true
        self.filter_retailers()

        # Step 1 - Skipped if output exists and CONFIG.ALLOW_SKIPPING is true
        self.match_retailers()

        # Step 2 - Skipped if output exists and CONFIG.ALLOW_SKIPPING is true
        self.post_matching_processing()

        # Step 3 - Skipped if output exists and CONFIG.ALLOW_SKIPPING is true
        self.create_overview()

        # Step 4 - Skipped if output exists and CONFIG.ALLOW_SKIPPING is true
        self.create_counts_stats()


if __name__ == "__main__":
    RetailerProcessor().run()
