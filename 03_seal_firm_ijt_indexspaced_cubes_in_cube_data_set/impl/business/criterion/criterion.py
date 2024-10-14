from typing import Protocol


class Criterion(Protocol):
    def apply(self, base_query: str) -> str:
        """
        Apply the criterion, possibly modifying the base SQL query (for joins, filters, etc.).
        """
        ...