class QueryBuilder:

    def __init__(
        self,
        table_name: str,
        select_columns: str = "*",
        is_distinct: bool = False,
        conditions: list = None,
        group_by_clause: str = "",
        order_by_clause: str = "",
        limit_clause: str = "",
        join_clause: str = "",
        window_clause: str = "",
        with_clause: str = "",
        having_clause: str = ""
    ):
        self.table_name = table_name
        self.select_columns = select_columns
        self.is_distinct = is_distinct
        self.conditions = conditions if conditions else []
        self.group_by_clause = group_by_clause
        self.order_by_clause = order_by_clause
        self.limit_clause = limit_clause
        self.join_clause = join_clause
        self.window_clause = window_clause
        self.with_clause = with_clause
        self.having_clause = having_clause

    def select(
        self,
        columns: list or str
    ):
        if isinstance(columns, list):
            self.select_columns = ", ".join(columns)
        else:
            self.select_columns = columns
        return self

    def distinct(self):
        self.is_distinct = True
        return self

    def where(
        self,
        condition: str
    ):
        self.conditions.append(condition)
        return self

    def group_by(
        self,
        columns: list or str
    ):
        if isinstance(columns, list):
            self.group_by_clause = f"GROUP BY {', '.join(columns)}"
        else:
            self.group_by_clause = f"GROUP BY {columns}"
        return self

    def having(
        self,
        condition: str
    ):
        self.having_clause = f"HAVING {condition}"
        return self

    def order_by(
        self,
        column: str,
        ascending: bool = True
    ):
        self.order_by_clause = f"ORDER BY {column} {'ASC' if ascending else 'DESC'}"
        return self

    def limit(
        self,
        n: int
    ):
        self.limit_clause = f"LIMIT {n}"
        return self

    def join(
        self,
        join_type: str,
        other_table: str,
        on_condition: str
    ):
        self.join_clause += f"{join_type.upper()} JOIN {other_table} ON {on_condition} "
        return self

    def window(
        self,
        window_definition: str
    ):
        self.window_clause = window_definition
        return self

    def with_cte(
        self,
        cte_name: str,
        cte_query: str
    ):
        if self.with_clause:
            self.with_clause += f", {cte_name} AS ({cte_query})"
        else:
            self.with_clause = f"WITH {cte_name} AS ({cte_query})"
        return self

    def build_where_clause(
        self,
        produkt_id: str = None,
        haendler_bez: str = None,
        time_start: str = None,
        time_end: str = None
    ):
        if produkt_id:
            self.where(f"produkt_id = '{produkt_id}'")
        if haendler_bez:
            self.where(f"haendler_bez = '{haendler_bez}'")
        if time_start is not None and time_end is not None:
            self.where(f"(dtimebegin <= {time_end} AND dtimeend >= {time_start})")
        elif time_start is not None:
            self.where(f"dtimeend >= {time_start}")
        elif time_end is not None:
            self.where(f"dtimebegin <= {time_end}")

        return self

    def _build_where_clause(self):
        return " AND ".join(self.conditions) if self.conditions else "1=1"

    def build(self) -> str:
        distinct_clause = 'DISTINCT' if self.is_distinct else ''
        where_clause = self._build_where_clause()

        query = (
            f"{self.with_clause} "
            f"SELECT {distinct_clause} {self.select_columns} "
            f"FROM {self.table_name} "
            f"{self.join_clause} "
            f"WHERE {where_clause} "
            f"{self.group_by_clause} "
            f"{self.having_clause} "
            f"{self.order_by_clause} "
            f"{self.limit_clause} "
            f"{self.window_clause}"
        ).strip()

        return query

    def insert_into(
        self,
        target_table: str
    ) -> str:
        select_query = self.build()
        insert_query = f"INSERT INTO {target_table} {select_query}"
        return insert_query
