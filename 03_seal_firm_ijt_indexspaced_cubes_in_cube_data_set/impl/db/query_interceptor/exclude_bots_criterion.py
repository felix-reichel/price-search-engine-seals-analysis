from impl.db.query_interceptor.interceptor_criterion import QueryInterceptionCriterion


class ExcludeBotsCriterion(QueryInterceptionCriterion):
    def apply(self, base_query: str) -> str:
        raise NotImplementedError
