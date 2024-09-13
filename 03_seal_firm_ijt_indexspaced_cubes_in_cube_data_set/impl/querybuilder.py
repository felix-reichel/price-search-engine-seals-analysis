class QueryBuilder:
    def __init__(self,
                 table_name):

        self.table_name = table_name
        self.select_columns = "*"
        self.conditions = []
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

    def order_by(self,
                 column,
                 ascending=True):

        self.order_by_clause = f"ORDER BY {column} {'ASC' if ascending else 'DESC'}"
        return self

    def limit(self,
              n):

        self.limit_clause = f"LIMIT {n}"
        return self

    def build(self):
        where_clause = " AND ".join(self.conditions) if self.conditions else "1=1"
        query = f"SELECT {self.select_columns} FROM {self.table_name} WHERE {where_clause} {self.order_by_clause} {self.limit_clause}"
        return query
