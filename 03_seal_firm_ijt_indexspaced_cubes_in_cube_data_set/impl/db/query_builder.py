class QueryBuilder:
    def __init__(self, table_name):
        self.table_name = table_name
        self.select_columns = "*"
        self.is_distinct = False
        self.conditions = []
        self.group_by_clause = ""
        self.order_by_clause = ""
        self.limit_clause = ""
        self.join_clause = ""
        self.window_clause = ""
        self.with_clause = ""
        self.having_clause = ""

    def select(self, columns):
        if isinstance(columns, list):
            self.select_columns = ", ".join(columns)
        else:
            self.select_columns = columns
        return self

    def distinct(self):
        self.is_distinct = True
        return self

    def where(self, condition):
        self.conditions.append(condition)
        return self

    def group_by(self, columns):
        if isinstance(columns, list):
            self.group_by_clause = f"GROUP BY {', '.join(columns)}"
        else:
            self.group_by_clause = f"GROUP BY {columns}"
        return self

    def having(self, condition):
        self.having_clause = f"HAVING {condition}"
        return self

    def order_by(self, column, ascending=True):
        self.order_by_clause = f"ORDER BY {column} {'ASC' if ascending else 'DESC'}"
        return self

    def limit(self, n):
        self.limit_clause = f"LIMIT {n}"
        return self

    def join(self, join_type, other_table, on_condition):
        self.join_clause += f"{join_type} JOIN {other_table} ON {on_condition} "
        return self

    def window(self, window_definition):
        self.window_clause = window_definition
        return self

    def with_cte(self, cte_name, cte_query):
        if self.with_clause:
            self.with_clause += f", {cte_name} AS ({cte_query})"
        else:
            self.with_clause = f"WITH {cte_name} AS ({cte_query})"
        return self

    def build_where_clause_i_j_t(self, produkt_id=None, haendler_bez=None, time_start=None, time_end=None):
        if produkt_id:
            self.conditions.append(f"produkt_id = '{produkt_id}'")
        if haendler_bez:
            self.conditions.append(f"haendler_bez = '{haendler_bez}'")
        if time_start is not None and time_end is not None:
            self.conditions.append(f"(dtimebegin <= {time_end} AND dtimeend >= {time_start})")
        elif time_start is not None:
            self.conditions.append(f"dtimeend >= {time_start}")  # Only end time condition
        elif time_end is not None:
            self.conditions.append(f"dtimebegin <= {time_end}")  # Only start time condition

        return self

    def build(self):
        distinct_clause = 'DISTINCT' if self.is_distinct else ''

        where_clause = " AND ".join(self.conditions) if self.conditions else "1=1"

        query = (f"{self.with_clause} "
                 f"SELECT {distinct_clause} {self.select_columns} "
                 f"FROM {self.table_name} "
                 f"{self.join_clause} "
                 f"WHERE {where_clause} "
                 f"{self.group_by_clause} "
                 f"{self.having_clause} "
                 f"{self.order_by_clause} "
                 f"{self.limit_clause} "
                 f"{self.window_clause}").strip()

        return query

    def insert_into(self, target_table):
        select_query = self.build()
        insert_query = f"INSERT INTO {target_table} {select_query}"
        return insert_query
