import pandas as pd
from CONFIG import *
from src.MatchingCriteria import MatchingCriteria


class MatchingProcessor:
    def __init__(self, filtered_retailers_df, matched_retailers_set):
        self.filtered_retailers_df = filtered_retailers_df
        self.matched_retailers_set = matched_retailers_set

    def match_and_append(self, guetesiegel_df, match_result_column, match_result_column_header, output_file,
                         matching_criteria):
        """Matches and appends retailers data to a DataFrame based on specified criteria."""
        guetesiegel_df.at[0, match_result_column] = match_result_column_header
        matches = 0
        for index, row in guetesiegel_df.iloc[1:].iterrows():
            matched_retailers = matching_criteria(row.iloc[1], self.filtered_retailers_df)
            if matched_retailers:
                if isinstance(matched_retailers, str):
                    guetesiegel_df.at[index, match_result_column] = matched_retailers
                    matches += 1
                    self.matched_retailers_set.add(matched_retailers)
                else:
                    guetesiegel_df.at[index, match_result_column] = str(matched_retailers)
                    matches += len(matched_retailers)
                    self.matched_retailers_set.update(matched_retailers)
        guetesiegel_df.to_csv(output_file, sep=CSV_SEPARATOR, index=False, header=None)
        return matches

    def process_matching(self, df, col_index, col_header, criteria, output_file):
        return self.match_and_append(df, col_index, col_header, output_file, criteria)

    @staticmethod
    def display_results(matches, initial_count, match_descriptions):
        print("Matches found:\n===")
        for desc, match in zip(match_descriptions, matches):
            print(f"{desc}: {match} out of {initial_count}\n===")


class ECommerceMatching(MatchingProcessor):
    def run_matching(self):
        initial_df = pd.read_csv(ECOMMERCE_GUETESIEGEL_FILE_PATH, sep=CSV_SEPARATOR, header=None)
        matches = [
            self.process_matching(initial_df, 7, "matching_criteria_simple",
                                  MatchingCriteria.matching_criteria_simple,
                                  ECOMMERCE_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(ECOMMERCE_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 8,
                                  HEADER_COLUMN_RESULTING_MATCH,
                                  MatchingCriteria.matching_criteria_simple,
                                  ECOMMERCE_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(ECOMMERCE_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 9,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_JARO_TOP_1,
                                  MatchingCriteria.matching_criteria_top_1_jaro_sim_candidate,
                                  ECOMMERCE_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(ECOMMERCE_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 10,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_JARO,
                                  MatchingCriteria.matching_criteria_closest_3_jaro_sim_candidates,
                                  ECOMMERCE_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(ECOMMERCE_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 11,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_1,
                                  MatchingCriteria.matching_criteria_advanced,
                                  ECOMMERCE_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(ECOMMERCE_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 12,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_2,
                                  MatchingCriteria.matching_criteria_advanced2,
                                  ECOMMERCE_MATCHED_FILE_PATH)
        ]

        self.display_results(matches, len(initial_df), [
            "Prefilled Match - Simple matching with high prob.",
            "Simple matching with high prob.",
            "JARO Match - Using Jaro similarity.",
            "Advanced matching - Using Jaro similarity.",
            "Matching Variant 1 - Advanced matching with lower prob. nchars=5 or nchars=3",
            "Matching Variant 2 - Advanced matching with lower prob. nchars=3 or nchars=4 if higher 'dot index'"
        ])


class EhiBevhMatching(MatchingProcessor):
    def run_matching(self):
        initial_df = pd.read_csv(EHI_BEVH_GUETESIEGEL_FILE_PATH, sep=CSV_SEPARATOR, header=None)
        matches = [
            self.process_matching(initial_df, 5, "matching_criteria_simple",
                                  MatchingCriteria.matching_criteria_simple,
                                  EHI_BEVH_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(EHI_BEVH_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 6,
                                  HEADER_COLUMN_RESULTING_MATCH,
                                  MatchingCriteria.matching_criteria_simple,
                                  EHI_BEVH_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(EHI_BEVH_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 7,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_JARO_TOP_1,
                                  MatchingCriteria.matching_criteria_top_1_jaro_sim_candidate,
                                  EHI_BEVH_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(EHI_BEVH_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 7,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_JARO,
                                  MatchingCriteria.matching_criteria_closest_3_jaro_sim_candidates,
                                  EHI_BEVH_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(EHI_BEVH_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 8,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_1,
                                  MatchingCriteria.matching_criteria_advanced,
                                  EHI_BEVH_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(EHI_BEVH_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 9,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_2,
                                  MatchingCriteria.matching_criteria_advanced2,
                                  EHI_BEVH_MATCHED_FILE_PATH)
        ]

        self.display_results(matches, len(initial_df), [
            "Prefilled Match - Simple matching with high prob.",
            "Simple matching with high prob.",
            "JARO Match - Using Jaro similarity.",
            "Advanced matching - Using Jaro similarity.",
            "Matching Variant 1 - Advanced matching with lower prob. nchars=5 or nchars=3",
            "Matching Variant 2 - Advanced matching with lower prob. nchars=3 or nchars=4 if higher 'dot index'"
        ])


class HandelsverbandMatching(MatchingProcessor):
    def run_matching(self):
        initial_df = pd.read_csv(HANDELSVERBAND_GUETESIEGEL_FILE_PATH, sep=CSV_SEPARATOR, header=None)
        matches = [
            self.process_matching(initial_df, 12, "matching_criteria_handelsverband",
                                  MatchingCriteria.matching_criteria_handelsverband,
                                  HANDELSVERBAND_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(HANDELSVERBAND_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 13,
                                  HEADER_COLUMN_RESULTING_MATCH,
                                  MatchingCriteria.matching_criteria_handelsverband,
                                  HANDELSVERBAND_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(HANDELSVERBAND_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 14,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_JARO_TOP_1,
                                  MatchingCriteria.matching_criteria_top_1_jaro_sim_candidate,
                                  HANDELSVERBAND_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(HANDELSVERBAND_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 15,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_JARO,
                                  MatchingCriteria.matching_criteria_closest_3_jaro_sim_candidates,
                                  HANDELSVERBAND_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(HANDELSVERBAND_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 16,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_1,
                                  MatchingCriteria.matching_criteria_advanced,
                                  HANDELSVERBAND_MATCHED_FILE_PATH),
            self.process_matching(pd.read_csv(HANDELSVERBAND_MATCHED_FILE_PATH, sep=CSV_SEPARATOR, header=None), 17,
                                  HEADER_COLUMN_NAME_ADVANCED_MATCHING_2,
                                  MatchingCriteria.matching_criteria_advanced2,
                                  HANDELSVERBAND_MATCHED_FILE_PATH)
        ]

        self.display_results(matches, len(initial_df), [
            "Prefilled Match - Simple matching with high prob.",
            "Simple matching with high prob.",
            "JARO Match - Using Jaro similarity.",
            "Advanced matching - Using Jaro similarity.",
            "Matching Variant 1 - Advanced matching with lower prob. nchars=5 or nchars=3",
            "Matching Variant 2 - Advanced matching with lower prob. nchars=3 or nchars=4 if higher 'dot index'"
        ])
