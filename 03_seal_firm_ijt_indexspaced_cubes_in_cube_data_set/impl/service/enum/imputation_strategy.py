from enum import Enum


class MeanImputationStrategy(Enum):
    NONE = "No imputation strategy, Missings allowed. (Will be denoted as EMPTY.)"  # -> ijt variant variable
    FIRM_LEVEL = "firm_level"  # -> j variant variable
    PRODUCT_LEVEL = "product_level"  # -> i variant variable
    FIRM_AND_TIME_LEVEL = "firm_and_time_level"  # -> -> jt variant variable
    FIRM_AND_PRODUCT_LEVEL = "firm_and_product_level"  # -> ji variant variable
    PRODUCT_AND_TIME_LEVEL = "product_and_time_level"  # -> it variant variable
    QUARTERLY_LEVEL = "QUARTERLY_LEVEL"  # -> q variant var.,  a variable can be an aggregate of t where q is a
    # quarterly variant var.
