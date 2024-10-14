from impl.business.criterion.criterion import Criterion


class ExcludeScrapperIpsCriterion(Criterion):  # criterion for clicks f.e.
    def apply(self, base_query: str) -> str:
        """
        Modify the query to exclude scrapper IPs by joining with the scrapper_ips table
        and filtering out matching rows based on the `user_ip` attribute.
        """
        return base_query + """
            LEFT JOIN scrapper_ips ON base_table.user_ip = scrapper_ips.user_ip
            WHERE scrapper_ips.user_ip IS NULL
        """