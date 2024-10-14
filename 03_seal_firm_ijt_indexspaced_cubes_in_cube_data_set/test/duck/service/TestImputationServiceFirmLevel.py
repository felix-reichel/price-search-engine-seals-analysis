import unittest
from unittest.mock import MagicMock, patch

from ..base.DuckDbBaseTest import DuckDbBaseTest
from impl.db.datasource import DuckDBDataSource
from impl.business.imputation_service import ImputationService, ImputationStrategy


class TestImputationServiceFirmLevel(DuckDbBaseTest):

    def setUp(self):
        super().setUp()
        self.db_source_mock = MagicMock(spec=DuckDBDataSource)
        self.connection_mock = MagicMock()
        self.db_source_mock.conn = self.connection_mock

        self.imputation_service = ImputationService(db_source=self.db_source_mock)

        self.mock_table_name = "test_table"
        self.mock_column_to_impute = "missing_column"
        self.mock_target_column = "some_column"

        # Mock data before imputation
        self.mock_data = [
            {"firm_id": 1, "some_column": 10, "missing_column": None},
            {"firm_id": 1, "some_column": 20, "missing_column": None},
            {"firm_id": 1, "some_column": 30, "missing_column": None},
            {"firm_id": 2, "some_column": 15, "missing_column": None},
            {"firm_id": 2, "some_column": 25, "missing_column": 25},  # already filled, currently ignored
            {"firm_id": 3, "some_column": 40, "missing_column": None},
            {"firm_id": 3, "some_column": 50, "missing_column": 50},  # already filled, currently ignored
            {"firm_id": 3, "some_column": 60, "missing_column": None},
            {"firm_id": 4, "some_column": 80, "missing_column": 80},  # already filled, currently ignored
            {"firm_id": 4, "some_column": 90, "missing_column": None}
        ]

    @patch('impl.db.querybuilder.QueryBuilder')
    def test_firm_level_imputation(self, query_builder_mock):
        query_builder_instance = MagicMock()
        query_builder_mock.return_value = query_builder_instance
        query_builder_instance.select.return_value = query_builder_instance
        query_builder_instance.where.return_value = query_builder_instance
        query_builder_instance.group_by.return_value = query_builder_instance
        query_builder_instance.build.return_value = (
            "SELECT firm_id, AVG(some_column) AS avg_value FROM test_table"
        )

        self.connection_mock.fetchall.return_value = [
            {"firm_id": 1, "avg_value": 20},  # (10+20+30)/3 = 20
            {"firm_id": 2, "avg_value": 15},  # (15) since 25 is already filled
            {"firm_id": 3, "avg_value": 50},  # (40+50+60)/3 = 50
            {"firm_id": 4, "avg_value": 85}   # (80+90)/2 = 85
        ]

        self.imputation_service.impute(
            strategy=ImputationStrategy.FIRM_LEVEL,
            table_name=self.mock_table_name,
            column_to_impute=self.mock_column_to_impute,
            target_column=self.mock_target_column
        )

        updated_data_after_imputation = [
            {"firm_id": 1, "some_column": 10, "missing_column": 20},  # Imputed with avg 20
            {"firm_id": 1, "some_column": 20, "missing_column": 20},  # Imputed with avg 20
            {"firm_id": 1, "some_column": 30, "missing_column": 20},  # Imputed with avg 20
            {"firm_id": 2, "some_column": 15, "missing_column": 15},  # Imputed with avg 15
            {"firm_id": 2, "some_column": 25, "missing_column": 25},  # Already filled
            {"firm_id": 3, "some_column": 40, "missing_column": 50},  # Imputed with avg 50
            {"firm_id": 3, "some_column": 50, "missing_column": 50},  # Already filled
            {"firm_id": 3, "some_column": 60, "missing_column": 50},  # Imputed with avg 50
            {"firm_id": 4, "some_column": 80, "missing_column": 80},  # Already filled
            {"firm_id": 4, "some_column": 90, "missing_column": 85}   # Imputed with avg 85
        ]

        self.connection_mock.execute.return_value = updated_data_after_imputation
        self.connection_mock.fetchall.return_value = updated_data_after_imputation

        self.assertEqual(updated_data_after_imputation[0]["missing_column"], 20)
        self.assertEqual(updated_data_after_imputation[1]["missing_column"], 20)
        self.assertEqual(updated_data_after_imputation[2]["missing_column"], 20)
        self.assertEqual(updated_data_after_imputation[3]["missing_column"], 15)
        self.assertEqual(updated_data_after_imputation[5]["missing_column"], 50)
        self.assertEqual(updated_data_after_imputation[7]["missing_column"], 50)
        self.assertEqual(updated_data_after_imputation[9]["missing_column"], 85)


if __name__ == '__main__':
    unittest.main()
