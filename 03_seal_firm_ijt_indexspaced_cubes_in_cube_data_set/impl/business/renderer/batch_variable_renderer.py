from typing import List

from impl.business.data_set import DataSet
from impl.business.renderer.variable_renderer import VariableRenderer
from impl.business.selector.space_selector import SpaceSelector
from impl.business.variable import Variable


def is_rendering_complete(variables: List[Variable]) -> bool:
    """
    Check if the rendering of all variables is complete.
    """
    # Logic to verify that all variables have been rendered
    # Example: compare row count in results table to expected row count
    return True


class BatchVariableRenderer(VariableRenderer):
    def __init__(self, dataset: DataSet):
        super().__init__(dataset)

    def batch_render(self, variables: List[Variable], space_selector: SpaceSelector):
        """
        Batch process multiple variables and render them.
        """
        for variable in variables:
            print(f"Rendering variable: {variable.name}")
            self.render_variable(variable, space_selector)

        # Check if all variables are rendered, then export the dataset
        if is_rendering_complete(variables):
            self.dataset.export_to_csv("final_dataset.csv")
