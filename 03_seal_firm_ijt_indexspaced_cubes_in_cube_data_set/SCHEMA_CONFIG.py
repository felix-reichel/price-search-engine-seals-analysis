# schema_config.py

INITIAL_TABLE_SCHEMAS = {
    "seal_change_firms": """
        CREATE TABLE IF NOT EXISTS seal_change_firms (
            seal_cp_filename STRING,
            seal_firm_id STRING,
            matched_haendler_bez STRING, 
            seal_date_str STRING,
            is_at BOOL,
            is_de BOOL,
            is_not_de_and_not_at BOOL
        )
    """,
    "filtered_haendler_bez": """
        CREATE TABLE IF NOT EXISTS filtered_haendler_bez (
            filtered_haendler_bez_id INT8,
            haendler_bez STRING
        )
    """,
    "products": """
        CREATE TABLE IF NOT EXISTS products (
            produkt_id BIGINT,
            dtime_birth BIGINT,
            dtime_death BIGINT,
            produkt_bez STRING,
            subsubkat STRING
        )
    """,
    "retailers": """
        CREATE TABLE IF NOT EXISTS retailers (
            haendler_bez STRING,
            is_at INT8,
            is_de INT8,
            is_uk INT8,
            is_nl INT8,
            laden INT8,
            abhol INT8,
            online INT8,
            lon FLOAT4,
            lat FLOAT4,
            versandk_default STRING,
            kunden_id BIGINT,
            mastercard INT8,
            visa INT8,
            amex INT8,
            dinersclub INT8,
            vk_at INT8,
            vk_de INT8,
            nn_at INT8,
            nn_de INT8,
            liefert_at INT8,
            liefert_de INT8,
            liefert_uk INT8,
            liefert_pl INT8,
            liefert_nl INT8,
            liefert_ie INT8,
            is_pl INT8,
            dtimeBegin BIGINT,
            dtimeEnd FLOAT4
        )
    """
}
