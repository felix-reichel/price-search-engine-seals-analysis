from functools import lru_cache

import CONFIG


class ApplicationThreadConfig:
    TOTAL_AVAILABLE_CPUS = CONFIG.NCPUS

    @staticmethod
    @lru_cache()
    def calculate_thread_distribution():  # Todo: furthermore To be patched in DuckDbBaseTest

        THREAD_PER_AVAIL_CORE = 1

        return {
            "duckdb_thread_count": 32,
            "duckdb_background_thread_count": 4,
        }

    @staticmethod
    def get_thread_config():
        return ApplicationThreadConfig.calculate_thread_distribution()

    @staticmethod
    def apply_thread_config(db_connection):
        config = ApplicationThreadConfig.calculate_thread_distribution()
        db_connection.conn.execute("SET allocator_background_threads=true;")
        db_connection.conn.execute(f"SET allocator_background_threads={config['duckdb_background_thread_count']};")
        db_connection.conn.execute(f"SET threads={config['duckdb_thread_count']};")
