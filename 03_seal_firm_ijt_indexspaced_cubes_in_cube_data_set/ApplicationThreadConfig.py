import math
from functools import lru_cache

import CONFIG


class ApplicationThreadConfig:
    MAX_CPU_USAGE_PERCENTAGE = CONFIG.MAX_CPU_USAGE_PERCENTAGE
    TOTAL_AVAILABLE_CPUS = CONFIG.NCPUS

    @staticmethod
    @lru_cache()
    def calculate_thread_distribution():
        max_usable_cpus = math.floor(
            ApplicationThreadConfig.TOTAL_AVAILABLE_CPUS * ApplicationThreadConfig.MAX_CPU_USAGE_PERCENTAGE
        )

        duckdb_thread_count = min(CONFIG.MAX_DUCKDB_THREADS, max_usable_cpus)
        background_thread_count = max(1, math.floor(duckdb_thread_count * 0.2))
        main_thread_count = min(CONFIG.SPAWN_MAX_MAIN_PROCESSES_AMOUNT, max_usable_cpus - background_thread_count)
        buffer_thread_count = max(1, max_usable_cpus - duckdb_thread_count - main_thread_count)

        return {
            "duckdb_thread_count": duckdb_thread_count,
            "background_thread_count": background_thread_count,
            "main_thread_count": main_thread_count,
            "buffer_thread_count": buffer_thread_count,
            "total_cpus": ApplicationThreadConfig.TOTAL_AVAILABLE_CPUS
        }

    @staticmethod
    def get_thread_config():
        return ApplicationThreadConfig.calculate_thread_distribution()

    @staticmethod
    def apply_thread_config(db_connection):
        config = ApplicationThreadConfig.calculate_thread_distribution()
        db_connection.conn.execute("SET allocator_background_threads=true;")
        db_connection.conn.execute(f"SET allocator_background_threads={config['background_thread_count']};")
        db_connection.conn.execute(f"SET threads={config['duckdb_thread_count']};")
