from enum import Enum


# All Variables in the data set are mostly aggregated outcome variables!
class VariableValueAggregationOrCollectionStrategy(Enum):
    SUM = "Uses SQL Sum() function. signaled verbally through 'total' at most"
    MIN = "Uses SQL Min()"
    MAX = "Uses SQL Max()"
    AVG = "Uses SQL Avg()"
    NEWEST_VALUE = "Sort by timestamp desc limit 1"
    OLDEST_VALUE = "Sort by timestamp asc limit 1"

class VariableRenderStrategy(Enum):
    OUTER_SPACE = "Target variable is rendered within outer cube target space."
    INNER_SPACE = "Target variable is rendered within inner cube target space."
    POSITIONAL_COORDINATES = "Target variable is rendered using positional coordinates."



