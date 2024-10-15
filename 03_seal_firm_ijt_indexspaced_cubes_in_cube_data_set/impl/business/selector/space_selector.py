from functools import lru_cache
from typing import List, Optional, Union

import polars as pl
from impl.business.enum.variable_render_strategy import VariableRenderStrategy

from impl.helpers import calculate_u_from_running_var_t


@lru_cache(maxsize=None)
def calculate_dtime_range_from_running_var(week_running_var: Union[int, List[int]]) -> (int, int):
    """
    Calculate dtimebegin and dtimeend from the week_running_var.
    If a list of week_running_var is provided, take the min and max values and map them to timestamps.
    """
    if isinstance(week_running_var, list):
        min_var = min(week_running_var)
        max_var = max(week_running_var)
    else:
        min_var = max_var = week_running_var

    dtimebegin = calculate_u_from_running_var_t(min_var)
    dtimeend = calculate_u_from_running_var_t(max_var)

    return dtimebegin, dtimeend


class SpaceSelector:
    def __init__(self,
                 haendler_bez: Optional[Union[str, List[str]]],
                 produkt_id: Optional[Union[str, List[str]]],
                 week_running_var: Optional[Union[int, List[int]]],
                 data: pl.DataFrame):
        """
        Initialize with values for haendler_bez, produkt_id, and week_running_var, which can be
        single values, lists of values, or None (indicating full outer space).
        Additionally, calculate dtimebegin and dtimeend from week_running_var if supplied.
        """
        self.haendler_bez = haendler_bez
        self.produkt_id = produkt_id
        self.week_running_var = week_running_var
        self.data = data

        # If week_running_var is supplied, calculate the corresponding timestamps
        if self.week_running_var is not None:
            self.dtimebegin, self.dtimeend = calculate_dtime_range_from_running_var(self.week_running_var)
        else:
            self.dtimebegin = None
            self.dtimeend = None

    def factorize_column(self, column_name: str, value: Optional[Union[str, List[str]]]) -> List[int]:
        """
        Convert a categorical string value (or list of values) into numeric factors based on the given data.
        If value is None, return the full range of factors for that column (i.e., the full outer space).
        """
        unique_values = self.data.select(pl.col(column_name).unique()).to_series().to_list()
        factor_map = {val: idx for idx, val in enumerate(unique_values)}

        if value is None:
            # Use the full outer space for this axis
            return list(range(len(unique_values)))
        elif isinstance(value, list):
            # Map list of values to factors
            return [factor_map[val] for val in value]
        else:
            # Map a single value to its factor
            return [factor_map[value]]

    def to_factor_representation(self) -> List[List[int]]:
        """
        Convert selection criteria values into lists of factorized integers (one list per axis).
        Each axis can represent multiple factorized values.
        """
        haendler_bez_factors = self.factorize_column('haendler_bez', self.haendler_bez)
        produkt_id_factors = self.factorize_column('produkt_id', self.produkt_id)
        week_running_var_factors = self.factorize_column('week_running_var', self.week_running_var)

        return [haendler_bez_factors, produkt_id_factors, week_running_var_factors]

    def determine_strategy(self, factors: List[int]) -> VariableRenderStrategy:
        """
        Determines the render strategy for a given axis based on the number of factors:
        - OUTER_SPACE: Full outer space (None was passed, or entire space is used).
        - INNER_SPACE: Multiple specific values (list of values).
        - POSITIONAL_COORDINATES: A single specific value (single value).
        """
        if len(factors) == len(self.data):
            return VariableRenderStrategy.OUTER_SPACE  # Full outer space
        elif len(factors) > 1:
            return VariableRenderStrategy.INNER_SPACE  # Multiple specific values
        else:
            return VariableRenderStrategy.POSITIONAL_COORDINATES  # Single specific value

    def determine_strategies(self) -> List[VariableRenderStrategy]:
        """
        Determine the render strategy for each axis: haendler_bez, produkt_id, and week_running_var.
        """
        factor_representation = self.to_factor_representation()
        strategies = []
        for factors in factor_representation:
            strategies.append(self.determine_strategy(factors))
        return strategies


def validate_selection_space(render_strategies: List[VariableRenderStrategy],
                             selection_criteria: SpaceSelector) -> bool:
    """
    Validate that the selection criteria conform to the render strategy on each axis.
    """
    factor_representation = selection_criteria.to_factor_representation()

    for i, axis_factors in enumerate(factor_representation):
        render_strategy = render_strategies[i]
        if render_strategy == VariableRenderStrategy.OUTER_SPACE:
            # OUTER_SPACE allows the full range of factors, but must be non-negative
            if not all(factor >= 0 for factor in axis_factors):
                return False
        elif render_strategy == VariableRenderStrategy.INNER_SPACE:
            # INNER_SPACE requires multiple factors, must be non-negative
            if len(axis_factors) <= 1 or not all(factor >= 0 for factor in axis_factors):
                return False
        elif render_strategy == VariableRenderStrategy.POSITIONAL_COORDINATES:
            # POSITIONAL_COORDINATES must ensure a single factor and be non-negative
            if len(axis_factors) != 1 or axis_factors[0] < 0:
                return False
    return True