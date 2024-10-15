# factory.py

from impl.db.datasource import DuckDBDataSource
from impl.repository.clicks_repository import ClicksRepository
from impl.repository.filtered_retailer_names_repository import FilteredRetailerNamesRepository
from impl.repository.offers_repository import OffersRepository
from impl.repository.seal_change_firms_repository import SealChangeFirmsDataRepository
from impl.service.clicks_service import ClicksService
from impl.service.mean_imputation_service import MeanImputationService
from impl.service.offers_service import OffersService


class Factory:
    _main_db_source = None
    _shadow_db_source = None

    @staticmethod
    def get_main_db_source() -> DuckDBDataSource:
        if Factory._main_db_source is None:
            Factory._main_db_source = DuckDBDataSource()
        return Factory._main_db_source

    @staticmethod
    def get_shadow_db_source() -> DuckDBDataSource:
        if Factory._shadow_db_source is None:
            Factory._shadow_db_source = DuckDBDataSource()
        return Factory._shadow_db_source

# Services
    @staticmethod
    def create_clicks_service() -> ClicksService:
        """
        Create and return a singleton instance of ClicksService with its dependencies injected.
        """
        db_source = Factory.get_main_db_source()
        repository = ClicksRepository(db_source)
        return ClicksService(repository)

    @staticmethod
    def create_offers_service() -> OffersService:
        """
        Create and return a singleton instance of OffersService with its dependencies injected.
        """
        db_source = Factory.get_main_db_source()
        repository = OffersRepository(db_source)
        return OffersService(repository)

    @staticmethod
    def create_mean_imputation_service() -> MeanImputationService:
        """
        Create and return a singleton instance of ImputationService with its dependencies injected.
        """
        db_source = Factory.get_main_db_source()
        return MeanImputationService(db_source)

# Repositories
    @staticmethod
    def create_filtered_retailer_names_repository() -> FilteredRetailerNamesRepository:
        """
        Create and return a singleton instance of FilteredRetailerNamesRepository.
        """
        db_source = Factory.get_main_db_source()
        return FilteredRetailerNamesRepository(db_source)

    @staticmethod
    def create_seal_change_firms_repository() -> SealChangeFirmsDataRepository:
        """
        Create and return a singleton instance of SealChangeFirmsDataRepository.
        """
        db_source = Factory.get_main_db_source()
        return SealChangeFirmsDataRepository(db_source)
