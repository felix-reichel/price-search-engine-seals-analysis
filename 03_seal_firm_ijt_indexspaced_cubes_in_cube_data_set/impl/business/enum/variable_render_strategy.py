from enum import Enum


class VariableRenderStrategy(Enum):
    OUTER_SPACE = "Target variable is rendered within outer cube target space."
    INNER_SPACE = "Target variable is rendered within inner cube target space."
    POSITIONAL_COORDINATES = "Target variable is rendered using positional coordinates."

