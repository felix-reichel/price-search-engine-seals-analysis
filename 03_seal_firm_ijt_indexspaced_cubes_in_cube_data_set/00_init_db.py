import logging

from impl.db.datasource import DuckDBDataSource
from impl.loaders.init_db import DatabaseInitializer

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='process_log.log',
    filemode='w'
)

logger = logging.getLogger(__name__)

if __name__ == '__main__':
    db = DuckDBDataSource()
    db_initializer = DatabaseInitializer(db)
    db_initializer.initialize_database()