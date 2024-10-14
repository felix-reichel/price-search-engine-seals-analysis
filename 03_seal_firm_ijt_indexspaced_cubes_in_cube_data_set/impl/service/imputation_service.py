from impl.db.datasource import DuckDBDataSource
from impl.db.querybuilder import QueryBuilder
from impl.service.enum.imputation_strategy import ImputationStrategy


class ImputationService:
    def __init__(self, db_source: DuckDBDataSource):
        """
        Initialize the ImputationService with a DuckDB data source.

        Parameters:
        db_source (DuckDBDataSource): The DuckDB data source instance.
        """
        self.db_source = db_source

    def impute(self, strategy: ImputationStrategy, table_name: str, column_to_impute: str, target_column: str):
        """
        Perform imputation based on the chosen strategy.

        Parameters:
        strategy (ImputationStrategy): The imputation strategy to use.
        table_name (str): The name of the table on which imputation is performed.
        column_to_impute (str): The column where imputation will be applied.
        target_column (str): The column to calculate the average value for imputation.
        """
        if strategy == ImputationStrategy.NONE:
            # Nothing to do
            pass

        elif strategy == ImputationStrategy.FIRM_LEVEL:
            self.impute_firm_level(table_name, column_to_impute, target_column)

        elif strategy == ImputationStrategy.PRODUCT_LEVEL:
            self.impute_product_level(table_name, column_to_impute, target_column)

        elif strategy == ImputationStrategy.FIRM_AND_TIME_LEVEL:
            self.impute_firm_and_unixtimestamp_level(table_name, column_to_impute, target_column)

        elif strategy == ImputationStrategy.FIRM_AND_PRODUCT_LEVEL:
            self.impute_firm_and_product_level(table_name, column_to_impute, target_column)

        elif strategy == ImputationStrategy.PRODUCT_AND_TIME_LEVEL:
            self.impute_product_unixtimestamp_level(table_name, column_to_impute, target_column)

    def impute_firm_level(self, table_name: str, column_to_impute: str, target_column: str):
        """
        Impute missing values at the firm level by averaging values of the target column.

        Parameters:
        table_name (str): The table to update.
        column_to_impute (str): The column where imputation is applied.
        target_column (str): The column to average for imputation.
        """
        query = (
            QueryBuilder(table_name=table_name)
            .select([f"haendler_bez, AVG({target_column}) AS avg_value"])
            .where(f"{column_to_impute} IS NULL")
            .group_by("haendler_bez")
            .build()
        )

        imputation_query = f"""
        UPDATE {table_name}
        SET {column_to_impute} = (SELECT avg_value FROM ({query}) WHERE haendler_bez = {table_name}.haendler_bez)
        WHERE {column_to_impute} IS NULL
        """
        self.db_source.conn.execute(imputation_query)

    def impute_product_level(self, table_name: str, column_to_impute: str, target_column: str):
        """
        Impute missing values at the product level by averaging values of the target column.

        Parameters:
        table_name (str): The table to update.
        column_to_impute (str): The column where imputation is applied.
        target_column (str): The column to average for imputation.
        """
        query = (
            QueryBuilder(table_name=table_name)
            .select([f"produkt_id, AVG({target_column}) AS avg_value"])
            .where(f"{column_to_impute} IS NULL")
            .group_by("produkt_id")
            .build()
        )

        imputation_query = f"""
        UPDATE {table_name}
        SET {column_to_impute} = (SELECT avg_value FROM ({query}) WHERE produkt_id = {table_name}.produkt_id)
        WHERE {column_to_impute} IS NULL
        """
        self.db_source.conn.execute(imputation_query)

    @NotImplementedError
    def impute_firm_and_unixtimestamp_level(self, table_name: str, column_to_impute: str, target_column: str):
        """
        Impute missing values at the firm and timestamp level by averaging values of the target column.

        Parameters:
        table_name (str): The table to update.
        column_to_impute (str): The column where imputation is applied.
        target_column (str): The column to average for imputation.
        """
        query = (
            QueryBuilder(table_name=table_name)
            .select([f"haendler_bez, unixtimestamp, AVG({target_column}) AS avg_value"])
            .where(f"{column_to_impute} IS NULL")
            .group_by(["haendler_bez", "unixtimestamp"])
            .build()
        )

        imputation_query = f"""
        UPDATE {table_name}
        SET {column_to_impute} = (SELECT avg_value FROM ({query}) WHERE haendler_bez = {table_name}.haendler_bez AND unixtimestamp = {table_name}.unixtimestamp)
        WHERE {column_to_impute} IS NULL
        """
        self.db_source.conn.execute(imputation_query)

    def impute_firm_and_product_level(self, table_name: str, column_to_impute: str, target_column: str):
        """
        Impute missing values at the firm and product level by averaging values of the target column.

        Parameters:
        table_name (str): The table to update.
        column_to_impute (str): The column where imputation is applied.
        target_column (str): The column to average for imputation.
        """
        query = (
            QueryBuilder(table_name=table_name)
            .select([f"haendler_bez, produkt_id, AVG({target_column}) AS avg_value"])
            .where(f"{column_to_impute} IS NULL")
            .group_by(["haendler_bez", "produkt_id"])
            .build()
        )

        imputation_query = f"""
        UPDATE {table_name}
        SET {column_to_impute} = (SELECT avg_value FROM ({query}) WHERE haendler_bez = {table_name}.haendler_bez AND produkt_id = {table_name}.produkt_id)
        WHERE {column_to_impute} IS NULL
        """
        self.db_source.conn.execute(imputation_query)

    @NotImplementedError
    def impute_product_unixtimestamp_level(self, table_name: str, column_to_impute: str, target_column: str):
        """
        Impute missing values at the product and timestamp level by averaging values of the target column.

        Parameters:
        table_name (str): The table to update.
        column_to_impute (str): The column where imputation is applied.
        target_column (str): The column to average for imputation.
        """
        query = (
            QueryBuilder(table_name=table_name)
            .select([f"produkt_id, unixtimestamp, AVG({target_column}) AS avg_value"])
            .where(f"{column_to_impute} IS NULL")
            .group_by(["produkt_id", "unixtimestamp"])
            .build()
        )

        imputation_query = f"""
        UPDATE {table_name}
        SET {column_to_impute} = (SELECT avg_value FROM ({query}) WHERE produkt_id = {table_name}.produkt_id AND unixtimestamp = {table_name}.unixtimestamp)
        WHERE {column_to_impute} IS NULL
        """
        self.db_source.conn.execute(imputation_query)
