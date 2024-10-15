import logging
from abc import ABC

from impl.singleton import Singleton

logger = logging.getLogger(__name__)


class AbstractBaseService(ABC, Singleton):
    def __init__(self, repository):
        """
        Initialize the service with a repository. This base class assumes each service has a repository.
        """
        if not hasattr(self, 'repository'):
            self.repository = repository
