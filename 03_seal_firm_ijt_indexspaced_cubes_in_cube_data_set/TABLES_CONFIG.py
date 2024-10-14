# TABLES_CONFIG.py

TABLES_CONFIG = {
    "ANGEBOTE_FOLDER_1": {
        "folder_name": "angebot_06_10",
        "drop_after_use": True,
        "row_limit": 1000000,  # Drop if table exceeds 1mil rows
        "cache_duration": 60,  # Drop after 60 minutes
        "file_log_delete": True  # Delete related file logs when dropping
    },
    "ANGEBOTE_FOLDER_2": {
        "folder_name": "angebot_11_15",
        "drop_after_use": True,
        "row_limit": 1000000,
        "cache_duration": 60,
        "file_log_delete": True
    },
    "ANGEBOTE_FOLDER_3": {
        "folder_name": "angebot",
        "drop_after_use": True,
        "row_limit": 2000000,  # Higher row limit for this folder
        "cache_duration": 60,
        "file_log_delete": True
    },

    # Clicks folder (should be dropped based on row limit)
    "CLICKS_FOLDER": {
        "folder_name": "clicks",
        "drop_after_use": True,
        "row_limit": 500000,  # Drop if more than 500k rows
        "cache_duration": 30,  # Drop after 30 minutes
        "file_log_delete": True
    },

    # Retailers file (should be kept in memory)
    "RETAILERS": {
        "file_name": "haendler.parquet",
        "drop_after_use": False,
        "row_limit": None,  # No row limit, table is kept
        "cache_duration": None,  # No cache expiration
        "file_log_delete": False
    },

    # Products file (should be kept in memory)
    "PRODUCTS": {
        "file_name": "produkt.parquet",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Scrapper IPs (should be kept)
    "SCRAPPER_IPS": {
        "file_name": "scrapper_ips.parquet",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Versand folders (should be dropped based on row limit)
    "VERSAND_11_15_FOLDER": {
        "folder_name": "/versand_11_15",
        "drop_after_use": True,
        "row_limit": 150000,
        "cache_duration": 60,
        "file_log_delete": True
    },
    "VERSAND_06_10_FOLDER": {
        "folder_name": "/versand_06_10",
        "drop_after_use": True,
        "row_limit": 150000,
        "cache_duration": 60,
        "file_log_delete": True
    },
    "VERSAND_FOLDER": {
        "folder_name": "/versand",
        "drop_after_use": True,
        "row_limit": 300000,
        "cache_duration": 60,
        "file_log_delete": True
    },

    # Verfügbarkeit folders (should be dropped based on row limit)
    "VERFUEGBARKEIT_11_15_FOLDER": {
        "folder_name": "/verfuegbarkeit_11_15",
        "drop_after_use": True,
        "row_limit": 100000,
        "cache_duration": 60,
        "file_log_delete": True
    },
    "VERFUEGBARKEIT_FOLDER": {
        "folder_name": "/verfuegbarkeit",
        "drop_after_use": True,
        "row_limit": 100000,
        "cache_duration": 60,
        "file_log_delete": True
    },

    # Product Specs folders (should be kept)
    "PROD_SPECS_SSC_FOLDER": {
        "folder_name": "/prod_specs_ssc",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },
    "PROD_SPECS_SC_FOLDER": {
        "folder_name": "/prod_specs_sc",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },
    "PROD_SPECS_CAT_FOLDER": {
        "folder_name": "/prod_specs_cat",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Product Specs Count (should be kept)
    "PRODUKT_SPECS_COUNT_FILE": {
        "file_name": "/produkt_specs_count.parquet",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Lookups folder (should be kept)
    "LOOKUPS_FOLDER": {
        "folder_name": "/lookups",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # LCT Cluster folder (should be kept)
    "LCT_CLUSTER_FOLDER": {
        "folder_name": "/lct_cluster",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Category folder (should be kept)
    "KATEGORIE_FOLDER": {
        "folder_name": "/kategorie",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Abfrage folders (should be kept)
    "ABFRAGE_PRODUKT_BEW_FOLDER": {
        "folder_name": "/abfrage_produkt_bew",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },
    "ABFRAGE_HAENDLER_BEW_FOLDER": {
        "folder_name": "/abfrage_haendler_bew",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },
    "ABFRAGE_FILTER_FOLDER": {
        "folder_name": "/abfrage_filter",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # SSC SC Cats file (should be kept)
    "SSC_SC_CATS_FILE": {
        "file_name": "/ssc_sc_cats.parquet",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Produktbewertung file (should be kept)
    "PRODUKTBEWERTUNG_FILE": {
        "file_name": "/produktbewertung.parquet",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Marken file (should be kept)
    "MARKEN_FILE": {
        "file_name": "/marken.parquet",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Produkt Marken file (should be kept)
    "PRODUKT_MARKEN_FILE": {
        "file_name": "/produkt_marken.parquet",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Händlerbewertung file (should be kept)
    "HAENDLERBEWERTUNG_FILE": {
        "file_name": "/haendlerbewertung.parquet",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Daily Händlerbewertung file (should be kept)
    "DAILY_HBEW_FILE": {
        "file_name": "/daily_hbew.parquet",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    },

    # Continuing Offers file (should be kept)
    "CONTINUING_OFFERS_FILE": {
        "file_name": "/continuing_offers.parquet",
        "drop_after_use": False,
        "row_limit": None,
        "cache_duration": None,
        "file_log_delete": False
    }
}
