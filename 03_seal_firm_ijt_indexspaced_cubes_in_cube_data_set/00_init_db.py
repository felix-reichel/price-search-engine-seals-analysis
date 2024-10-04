import logging

from impl.db.datasource import DuckDBDataSource
from impl.db.loaders.init_db import DatabaseInitializer

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', filename='00_init_db.log',
                    filemode='w')
logger = logging.getLogger(__name__)

if __name__ == '__main__':
    db = DuckDBDataSource()
    db_initializer = DatabaseInitializer(db)
    db_initializer.initialize_database()
