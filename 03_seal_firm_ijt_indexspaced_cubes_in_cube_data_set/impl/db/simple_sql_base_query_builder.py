class SimpleSQLBaseQueryBuilder:
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
        """
        Initialize the QueryBuilder with the basic SQL query components.

        Parameters:
        table_name (str): Name of the table to query.
        select_columns (str): Columns to select (default is '*').
        is_distinct (bool): Whether to include DISTINCT in the query.
        conditions (list): WHERE conditions for filtering results.
        group_by_clause (str): GROUP BY clause.
        order_by_clause (str): ORDER BY clause.
        limit_clause (str): LIMIT clause.
        join_clause (str): JOIN clause.
        window_clause (str): WINDOW clause for window functions.
        with_clause (str): WITH clause for Common Table Expressions (CTEs).
        having_clause (str): HAVING clause for filtering after GROUP BY.
        """
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
        """
        Set the columns to select in the query.

        Parameters:
        columns (list or str): List of column names or a single column as a string.
        """
        if isinstance(columns, list):
            self.select_columns = ", ".join(columns)
        else:
            self.select_columns = columns
        return self

    def distinct(self):
        """
        Add DISTINCT to the query.
        """
        self.is_distinct = True
        return self

    def where(
            self,
            condition: str
    ):
        """
        Add a condition to the WHERE clause.

        Parameters:
        condition (str): The condition to be added to the WHERE clause.
        """
        self.conditions.append(condition)
        return self

    def group_by(
            self,
            columns: list or str
    ):
        """
        Set the GROUP BY clause.

        Parameters:
        columns (list or str): List of column names or a single column as a string.
        """
        if isinstance(columns, list):
            self.group_by_clause = f"GROUP BY {', '.join(columns)}"
        else:
            self.group_by_clause = f"GROUP BY {columns}"
        return self

    def having(
            self,
            condition: str
    ):
        """
        Set the HAVING clause for the query.

        Parameters:
        condition (str): The condition to be added to the HAVING clause.
        """
        self.having_clause = f"HAVING {condition}"
        return self

    def order_by(
            self,
            column: str,
            ascending: bool = True
    ):
        """
        Set the ORDER BY clause for the query.

        Parameters:
        column (str): The column to order the results by.
        ascending (bool): Whether to order by ascending or descending (default is True).
        """
        self.order_by_clause = f"ORDER BY {column} {'ASC' if ascending else 'DESC'}"
        return self

    def limit(
            self,
            n: int
    ):
        """
        Set the LIMIT clause for the query.

        Parameters:
        n (int): The number of rows to limit the results to.
        """
        self.limit_clause = f"LIMIT {n}"
        return self

    def join(
            self,
            join_type: str,
            other_table: str,
            on_condition: str
    ):
        """
        Add a JOIN clause to the query.

        Parameters:
        join_type (str): The type of join (e.g., INNER, LEFT).
        other_table (str): The name of the other table to join with.
        on_condition (str): The condition on which to join the tables.
        """
        self.join_clause += f"{join_type.upper()} JOIN {other_table} ON {on_condition} "
        return self

    def window(
            self,
            window_definition: str
    ):
        """
        Set the window clause for window functions.

        Parameters:
        window_definition (str): The window function definition.
        """
        self.window_clause = window_definition
        return self

    def with_cte(
            self,
            cte_name: str,
            cte_query: str
    ):
        """
        Add a Common Table Expression (CTE) to the query.

        Parameters:
        cte_name (str): The name of the CTE.
        cte_query (str): The query to define the CTE.
        """
        if self.with_clause:
            self.with_clause += f", {cte_name} AS ({cte_query})"
        else:
            self.with_clause = f"WITH {cte_name} AS ({cte_query})"
        return self

    def build_ijt_where_clause(
            self,
            produkt_id: str = None,
            haendler_bez: str = None,
            time_start: str = None,
            time_end: str = None
    ):
        """
        Dynamically build the WHERE clause based on provided filters.

        Parameters:
        produkt_id (str, optional): Product ID filter.
        haendler_bez (str, optional): Retailer name filter.
        time_start (str, optional): Start time filter.
        time_end (str, optional): End time filter.
        """
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
        """
        Internal method to build the complete WHERE clause from conditions.
        """
        return " AND ".join(self.conditions) if self.conditions else "1=1"

    def build(self) -> str:
        """
        Build the complete SQL query.

        Returns:
        str: The complete SQL query.
        """
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
        """
        Build an INSERT INTO query.

        Parameters:
        target_table (str): The target table to insert data into.

        Returns:
        str: The complete INSERT INTO query.
        """
        select_query = self.build()
        insert_query = f"INSERT INTO {target_table} {select_query}"
        return insert_query
