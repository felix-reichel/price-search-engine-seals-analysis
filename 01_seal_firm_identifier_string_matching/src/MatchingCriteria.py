from CONFIG import GUETESIEGEL_NAME_SUFFIXES, INSUFFICIENT_RETAILER_MATCH_SUFFIXES, GUETESIEGEL_NAME_PREFIXES
from static.StringHelper import StringHelper


class MatchingCriteria:
    @staticmethod
    def matching_criteria_handelsverband(guetesiegel_retailer_name, filtered_retailers_df):
        """Matching criteria for Handelsverband."""
        if guetesiegel_retailer_name.startswith("www."):
            possible_match = guetesiegel_retailer_name.replace('.', '-').replace("www-", "")
            return possible_match if possible_match in filtered_retailers_df[1].values else None
        else:
            return MatchingCriteria.matching_criteria_simple(guetesiegel_retailer_name, filtered_retailers_df)

    @staticmethod
    def matching_criteria_simple(guetesiegel_retailer_name, filtered_retailers_df):
        """Simple matching criteria."""
        stripped_name = StringHelper.strip_prefixes_suffixes(guetesiegel_retailer_name, None, GUETESIEGEL_NAME_SUFFIXES)
        possible_match = stripped_name.replace('.', '-')
        return possible_match if possible_match in filtered_retailers_df[1].values else None

    @staticmethod
    def _matching_criteria_advanced(guetesiegel_retailer_name, filtered_retailers_df, nchars):
        """Internal method for advanced matching criteria."""
        stripped_name = StringHelper.strip_prefixes_suffixes(guetesiegel_retailer_name, GUETESIEGEL_NAME_PREFIXES,
                                                             GUETESIEGEL_NAME_SUFFIXES)
        matches = []
        for retailer in filtered_retailers_df[1].values:
            substring_front = stripped_name[:nchars]
            substring_end = stripped_name[-nchars:]
            if (
                    retailer.startswith(substring_front)
                    or (
                        retailer.endswith(substring_end)
                        and all(insufficient_suffix not in substring_end for insufficient_suffix in
                                INSUFFICIENT_RETAILER_MATCH_SUFFIXES)
                        and substring_end not in INSUFFICIENT_RETAILER_MATCH_SUFFIXES
                    )
            ):
                matches.append(retailer)
        return matches

    @staticmethod
    def matching_criteria_advanced(guetesiegel_retailer_name, filtered_retailers_df):
        """Advanced matching criteria."""
        nchars = 3 if len(guetesiegel_retailer_name) < 6 else 5
        return MatchingCriteria._matching_criteria_advanced(guetesiegel_retailer_name, filtered_retailers_df, nchars)

    @staticmethod
    def matching_criteria_advanced2(guetesiegel_retailer_name, filtered_retailers_df):
        """Advanced matching criteria."""
        nchars = 3 if guetesiegel_retailer_name.find('.') <= 3 else 4
        return MatchingCriteria._matching_criteria_advanced(guetesiegel_retailer_name, filtered_retailers_df, nchars)

    @staticmethod
    def matching_criteria_closest_3_jaro_sim_candidates(guetesiegel_retailer_name, filtered_retailers_df):
        """Matching criteria based on the closest 3 candidates using string distance."""
        distances = [
            (name, StringHelper.jaro_similarity(guetesiegel_retailer_name, name)) for name in
            filtered_retailers_df[1].values
        ]
        distances.sort(key=lambda x: x[1], reverse=True)
        return [name for name, _ in distances[:3]] if distances else None

    @staticmethod
    def matching_criteria_top_1_jaro_sim_candidate(guetesiegel_retailer_name, filtered_retailers_df):
        """Matching criteria based on the closest candidate using Jaro similarity."""
        best_similarity = 0.0
        best_candidate = None
        for retailer_name in filtered_retailers_df[1].values:
            similarity = StringHelper.jaro_similarity(guetesiegel_retailer_name, retailer_name)
            if similarity > best_similarity:
                best_similarity = similarity
                best_candidate = retailer_name
        return best_candidate
