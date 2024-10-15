from typing import Protocol


class QueryInterceptionCriterion(Protocol):
    def apply(self, base_query: str) -> str:
        """
        Apply the query_interceptor, possibly modifying the base SQL query (for joins, filters, etc.).
        """
        # can be applied, TODO: has to happen in an ordaaaaa'red manner!
        #
