from typing import Optional, List

import polars as pl

from impl.business.data_set import DataSet
from impl.business.enum.variable import VariableRenderStrategy
from impl.business.enum.variable import VariableValueAggregationOrCollectionStrategy
from impl.business.selector.space_selector import SpaceSelector
from impl.db.query_interceptor.interceptor_criterion import QueryInterceptionCriterion
from impl.service.enum.imputation_strategy import MeanImputationStrategy
from impl.service.mean_imputation_service import MeanImputationService


class Variable:     # What is a variable?
    def __init__(self,
                 label: str,  # the variable label -> to be stored into meta data (maybe)
                 label_description: str,  # the variable (label) description

                 # can have a list of intercepting criterions that will cause join, filter logic on lower lever
                 interception_criterions: Optional[List[QueryInterceptionCriterion]] = None,

                 # All variables in the data set are aggregates over its local space (), default is SUM (total amount)
                 aggregation_strategy: VariableValueAggregationOrCollectionStrategy
                 = VariableValueAggregationOrCollectionStrategy.SUM,

                 # A variable can be imputed.
                 # This is only allowed once fully rendered / all realizations exist!!!
                 # TODO: check is rendered  / is completed
                 imputation_strategy: Optional[MeanImputationStrategy] = None,

                 # Render strategy for the variable to be applied
                 # this converts ?
                 # has to be a feasible combination with VariableAggregationStrategy
                 # most likely inner space, which refers to a local space where t can have 52 values, i and j are lists.
                 render_strategy: Optional[VariableRenderStrategy] = VariableRenderStrategy.INNER_SPACE):
        """
        Abstraction for a variable, containing the name, description, optional criterions (which trigger joins on other tables),
        optional imputation strategy, and a render strategy.
        """
        self.label = label
        self.label_description = label_description
        self.interception_criterions = interception_criterions if interception_criterions else []  # Criterions will cause joins or filters

        self.aggregation_strategy = aggregation_strategy
        self.imputation_strategy = imputation_strategy
        self.render_strategy = render_strategy  # Each variable can have its own render strategy

    def load_variable_data(self, dataset: DataSet, space_selector: SpaceSelector) -> pl.DataFrame:
        """
        Load data for this variable based on the space selection criteria.
        This method also handles joins and filters based on criterions.
        """
        base_query = f"SELECT * FROM {dataset.db_source} WHERE variable_name = '{self.label}' AND space in {space_selector}"

        # Apply each query_interceptor to modify the query (joins, exclusions, etc.)
        for criterion in self.criterions:
            base_query = criterion.apply(base_query)

        return dataset.execute_query(base_query)

    def impute(self, dataset: DataSet, space_selector: SpaceSelector):
        """
        Impute missing data for this variable using the defined imputation strategy.
        """
        if self.imputation_strategy is not None:
            MeanImputationService(dataset.db_source).impute(self.imputation_strategy, 'table_name',
                                                            'target_col', '')

    def determine_render_strategy(self, space_selector: 'SpaceSelector') -> VariableRenderStrategy:
        """
        Determine the render strategy for this variable based on the space selection.
        """
        # If variable has its render strategy, we return it
        return self.render_strategy
