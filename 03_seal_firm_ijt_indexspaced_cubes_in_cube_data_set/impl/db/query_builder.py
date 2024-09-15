class QueryBuilder:
    def __init__(self,
                 table_name):

        self.table_name = table_name
        self.select_columns = "*"
        self.conditions = []
        self.group_by_clause = ""
        self.order_by_clause = ""
        self.limit_clause = ""

    def select(self,
               columns):

        if isinstance(columns, list):
            self.select_columns = ", ".join(columns)
        else:
            self.select_columns = columns
        return self

    def where(self,
              condition):

        self.conditions.append(condition)
        return self

    def group_by(self, columns):
        if isinstance(columns, list):
            self.group_by_clause = f"GROUP BY {', '.join(columns)}"
        else:
            self.group_by_clause = f"GROUP BY {columns}"
        return self

    def order_by(self,
                 column,
                 ascending=True):

        self.order_by_clause = f"ORDER BY {column} {'ASC' if ascending else 'DESC'}"
        return self

    def limit(self,
              n):

        self.limit_clause = f"LIMIT {n}"
        return self

    def build_where_clause_i_j_t(self,
                                 produkt_id=None,
                                 haendler_bez=None,
                                 time_start=None,
                                 time_end=None):

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
        where_clause = " AND ".join(self.conditions) if self.conditions else "1=1"
        query = f"SELECT {self.select_columns} FROM {self.table_name} WHERE {where_clause} {self.group_by_clause} {self.order_by_clause} {self.limit_clause}"
        return query

    def insert_into(self, target_table):
        select_query = self.build()

        insert_query = f"INSERT INTO {target_table} {select_query}"

        return insert_query
