# Variable Renderer Class
from impl.business.enum.variable_render_strategy import VariableRenderStrategy

from impl.business.data_set import DataSet
from impl.business.selector.space_selector import SpaceSelector
from impl.business.variable import Variable


class VariableRenderer:
    def __init__(self, dataset: DataSet):
        self.dataset = dataset

    def render_variable(self, variable: Variable, space_selector: SpaceSelector):
        """
        Render the given variable in the specified space.
        """
        # Load variable data based on space selection
        variable_data = variable.load_variable_data(self.dataset, space_selector)

        # Apply the imputation strategy if necessary
        variable.impute(self.dataset, space_selector)

        # Determine the render strategy for this variable and perform actions based on it
        render_strategy = variable.determine_render_strategy(space_selector)

        # Handle rendering based on strategy
        if render_strategy == VariableRenderStrategy.OUTER_SPACE:
            print(f"Rendering variable {variable.label} in OUTER_SPACE")
            # Process data as full outer space (no filtering)
        elif render_strategy == VariableRenderStrategy.INNER_SPACE:
            print(f"Rendering variable {variable.label} in INNER_SPACE")
            # Process data with multiple specific values
        elif render_strategy == VariableRenderStrategy.POSITIONAL_COORDINATES:
            print(f"Rendering variable {variable.label} in POSITIONAL_COORDINATES")
            # Process data for single specific value

        # Append results back to the dataset
        self.dataset.append_data(variable_data)