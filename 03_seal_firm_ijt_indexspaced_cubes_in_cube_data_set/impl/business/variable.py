from typing import Optional, List

import polars as pl

from impl.business.criterion.criterion import Criterion
from impl.business.data_set import DataSet
from impl.business.enum.variable_render_strategy import VariableRenderStrategy
from impl.service.enum.imputation_strategy import ImputationStrategy
from impl.service.imputation_service import ImputationService


class Variable:
    def __init__(self, name: str, description: str, criterions: Optional[List[Criterion]] = None,
                 imputation_strategy: Optional[ImputationStrategy] = None,
                 render_strategy: Optional[VariableRenderStrategy] = VariableRenderStrategy.OUTER_SPACE):
        """
        Abstraction for a variable, containing the name, description, optional criterions (which trigger joins on other tables),
        optional imputation strategy, and a render strategy.
        """
        self.name = name
        self.description = description
        self.criterions = criterions if criterions else []  # Criterions will cause joins or filters
        self.imputation_strategy = imputation_strategy
        self.render_strategy = render_strategy  # Each variable can have its own render strategy

    def load_variable_data(self, dataset: DataSet, space_selector: 'SpaceSelector') -> pl.DataFrame:
        """
        Load data for this variable based on the space selection criteria.
        This method also handles joins and filters based on criterions.
        """
        base_query = f"SELECT * FROM {dataset.db_source} WHERE variable_name = '{self.name}' AND space in {space_selector}"

        # Apply each criterion to modify the query (joins, exclusions, etc.)
        for criterion in self.criterions:
            base_query = criterion.apply(base_query)

        return dataset.execute_query(base_query)

    def impute(self, dataset: DataSet, space_selector: 'SpaceSelector'):
        """
        Impute missing data for this variable using the defined imputation strategy.
        """
        if self.imputation_strategy is not None:
            ImputationService(dataset.db_source).impute(self.imputation_strategy, space_selector)

    def determine_render_strategy(self, space_selector: 'SpaceSelector') -> VariableRenderStrategy:
        """
        Determine the render strategy for this variable based on the space selection.
        """
        # If variable has its render strategy, we return it
        return self.render_strategy
