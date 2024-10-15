from impl.db.query_interceptor.interceptor_criterion import QueryInterceptionCriterion


class ExcludeScrapperIpsCriterion(QueryInterceptionCriterion):  # query_interceptor for clicks f.e.
    def apply(self, base_query: str) -> str:
        """
        Modify the query to exclude scrapper IPs by joining with the scrapper_ips table
        and filtering out matching rows based on the `user_ip` attribute.
        """
        return base_query + """
            LEFT JOIN scrapper_ips ON base_table.user_ip = scrapper_ips.user_ip
            WHERE scrapper_ips.user_ip IS NULL
        """