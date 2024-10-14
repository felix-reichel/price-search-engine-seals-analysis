from impl.business.criterion.criterion import Criterion


class ExcludeBotsCriterion(Criterion):
    def apply(self, base_query: str) -> str:
        raise NotImplementedError
