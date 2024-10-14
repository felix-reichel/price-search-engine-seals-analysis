# factories/service_factory.py

import CONFIG
from impl.db.datasource import DuckDBDataSource
from impl.repository.clicks_repository import ClicksRepository
from impl.repository.filtered_retailer_names_repository import FilteredRetailerNamesRepository
from impl.repository.offers_repository import OffersRepository
from impl.repository.seal_change_firms_repository import SealChangeFirmsDataRepository
from impl.service.clicks_service import ClicksService
from impl.business.imputation_service import ImputationService
from impl.service.offers_service import OffersService


class ServiceFactory:
    _db_source = None

    @staticmethod
    def get_db_source() -> DuckDBDataSource:
        if ServiceFactory._db_source is None:
            ServiceFactory._db_source = DuckDBDataSource(CONFIG.DUCKDB_PATH)
        return ServiceFactory._db_source

    @staticmethod
    def create_clicks_service() -> ClicksService:
        """
        Create and return a singleton instance of ClicksService with its dependencies injected.
        """
        db_source = ServiceFactory.get_db_source()
        repository = ClicksRepository(db_source)
        return ClicksService(repository)

    @staticmethod
    def create_offers_service() -> OffersService:
        """
        Create and return a singleton instance of OffersService with its dependencies injected.
        """
        db_source = ServiceFactory.get_db_source()
        repository = OffersRepository(db_source)
        return OffersService(repository)

    @staticmethod
    def create_imputation_service() -> ImputationService:
        """
        Create and return a singleton instance of ImputationService with its dependencies injected.
        """
        db_source = ServiceFactory.get_db_source()
        return ImputationService(db_source)

    @staticmethod
    def create_filtered_retailer_names_repository() -> FilteredRetailerNamesRepository:
        """
        Create and return a singleton instance of FilteredRetailerNamesRepository.
        """
        db_source = ServiceFactory.get_db_source()
        return FilteredRetailerNamesRepository(db_source)

    @staticmethod
    def create_seal_change_firms_repository() -> SealChangeFirmsDataRepository:
        """
        Create and return a singleton instance of SealChangeFirmsDataRepository.
        """
        db_source = ServiceFactory.get_db_source()
        return SealChangeFirmsDataRepository(db_source)
