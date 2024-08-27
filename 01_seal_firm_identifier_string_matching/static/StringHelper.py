import re

from jellyfish import jaro_similarity


class StringHelper:
    @staticmethod
    def strip_prefixes_suffixes(text, prefixes=None, suffixes=None):
        """Strips prefixes and suffixes from text."""
        if prefixes:
            for prefix in prefixes:
                if text.startswith(prefix):
                    text = text[len(prefix):]
                    break
        if suffixes:
            for suffix in suffixes:
                if text.endswith(suffix):
                    text = text[:-len(suffix)]
                    break
        return text

    @staticmethod
    def jaro_similarity(str1, str2):
        """Calculates the Jaro distance between two strings."""
        return jaro_similarity(str1, str2)

    @staticmethod
    def extract_year(date_str):
        # Regex to find four consecutive digits (likely year)
        match = re.search(r'\b(19|20)\d{2}\b', date_str)
        if match:
            return int(match.group(0))
        return None
