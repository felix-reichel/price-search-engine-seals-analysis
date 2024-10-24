# CONFIG.py
import os
from pathlib import Path

# DEFINITION OF GLOBAL CONSTANTS #
os.environ["POLARS_MAX_THREADS"] = "64"
os.environ["NUMEXPR_MAX_THREADS"] = "32"
os.environ["ARROW_NUM_THREADS"] = "8"

NCPUS = os.getenv("NCPUS")  # should be available
# else CPU_COUNT = os.cpu_count()
# or psutil.cpu_count(logical=True)

# DUCKDB CONFIG
DUCKDB_PATH = ':memory:'
DUCKDB_MEMORY_LIMIT = '1200GB'   # TODO: Set based on curr Caps
MAX_DUCKDB_THREADS = 128  # test
MAX_DUCKDB_BACKGROUND_THREADS = 32

# PYTHON CONFIG
# Processors
SPAWN_MAX_MAIN_PROCESSES_AMOUNT = 8 # todo: test

# UNIX Time Constants
UNIX_HOUR = 60 * 60
UNIX_DAY = 60 * 60 * 24
UNIX_WEEK = 7 * 24 * 60 * 60  # 604800
UNIX_MONTH = 2629743  # 1 Month (30.44 days), see https://www.unixtimestamp.com/ for details
UNIX_YEAR = 31556926  # 1 Year (365.24 days), see https://www.unixtimestamp.com/ for details

UNIX_WEDNESDAY_MIDDAY_INTERCEPT = UNIX_DAY * 2.5  # From Monday 0:00 GMT+0000

# FULL TAU

# UNIX TIME ORIGIN u0 from which the running variable t populates
# A feasible time origin might be the first Unix time of all retailers or products.
# The time origin of products according to produkt.parquet is 1145814095 (Sun Apr 23 2006 17:41:35 GMT+0000).
# The time origin of haendler is 1179027421 (Sun May 13 2007 03:37:01 GMT+0000).
# Therefore, Seal Changes approx. >= July 2007 are relevant.
# UNIX_TIME_ORIGIN (Mon May 14 2007 00:00:00 GMT+0200)
UNIX_TIME_ORIGIN = 1179093600

UNIX_TIME_ORIGIN_FIRST_WEEK_WEDNESDAY = UNIX_TIME_ORIGIN + UNIX_WEDNESDAY_MIDDAY_INTERCEPT
# UNIX_TIME_ORIGIN + t=8 weeks
UNIX_TIME_ORIGIN_T8_INTERCEPT = UNIX_TIME_ORIGIN + 8 * UNIX_WEEK

# UNIX TIME COLLAPSE u1 until the running variable t populates
# Because we observe seal changes (S) of firms until 2022, and we want to look 1 year after,
# the Unix time runs until the end of 2023, which yields:
# Sun Dec 31 2023 00:00:00 GMT+0100
UNIX_TIME_COLLAPSE = 1703977200

# OBSERVATIONAL UNIT SELECTION CRITERIA PARAMS

# n ~ firms x TOP200 prods x (10 cf +1 main) x 52 (T Trunc sym.)
# FIRST COLS SPEC

# i
# j
# t
# u(t) ... representation
# t_real_seal
# is_seal_firm_j ... if not => counterfactual firm
# obs_id(i,j,t)
# clicks_ijt
# lct_ijt
# ...
# Var_ijt, Var_ij, Var_{ijt}, Var_{{space}}
# IV_{{space}}

# Loaders
OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED = 52
OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED = 26

# Sampler
RANDOM_SAMPLER_DETERMINISTIC_SEED = 42

# Sampler Params
RANDOM_PRODUCTS_AMOUNTS = 50  # 50 previously doesn't return any valid prods for firm 1
RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT = 10  # + 1 from the firm looking

# Params
MAX_TIME_WINDOW_WEEKS_AROUND_SEAL_WEEKS_AMOUNT = 52

# Forbidden Retailer Keywords
FORBIDDEN_RETAILER_KEYWORDS = [
    '-am-uk', '-am-de', '-am-at', '-eb-uk', '-eb-de', '-sh-at', '-mp-de',
    '-rk-de', '-nk-pl', '-sz-uk', '-vk-de', '-gx-de'
]
TOP_PRODUCTS_OF_SEAL_CHANGE_FIRM_BY_CLICKS_AMOUNT = 200  # Top 200 products i of seal change firm
HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT = 4  # 8 weeks before and 8 weeks after
MAX_WEEKS_BEFORE_AFTER_PRODUCT_ANGEBOTEN_MISSING_ALLOWED_AMOUNT = 1  # 1 week missing is allowed
MAX_DAYS_ANGEBOT_MISSING_WITHIN_WEEK = 0  # 0 = "Durchgehend angeboten", product must have an offer each day in a week

# CSV FILE SETTINGS
CSV_IMPORT_DELIM_STYLE = ";"
CSV_OUTPUT_DELIM_STYLE = ","

# STATIC INPUT FILES
SEAL_CHANGE_DATE_PATTERN = '%d.%m.%Y'

# GLOBAL FILE PATHS
PARQUET_FILES_DIR = Path("/nfn_vwl/geizhals/zieg_pq_db")
# PARQUET_FILES_DIR = Path("./data")

# SINGLE FILE PATHS

# FILTERED HAENDLER BEZ (see project FR-01, refer to filtered_haendler_bez.csv)
FILTERED_HAENDLER_BEZ = './data/filtered_haendler_bez.csv'  # Contains j ∈ { J_G, J_C }

# SEAL CHANGE FIRMS J_G, seal change data t_sealchange, and seal provider names
SEAL_CHANGE_FIRMS = './data/final_matrix.csv'  # Contains j ∈ { J_G } in the column RESULTING MATCH
# and t_seal_change in the column Guetesiegel First Date in the format of 12.07.2007 until row 255
# the seal provider is in the first column

# PARQUET DATABASE FILES

# ANGEBOTE FOLDERS
ANGEBOTE_FOLDER_1 = "angebot_06_10"
ANGEBOTE_FOLDER_2 = "angebot_11_15"
ANGEBOTE_FOLDER_3 = "angebot"
ANGEBOTE_FOLDER = [ANGEBOTE_FOLDER_1, ANGEBOTE_FOLDER_2, ANGEBOTE_FOLDER_3]
# [angebot_id] BIGINT NULL,
# [produkt_id] BIGINT NULL,
# [haendler_bez] NVARCHAR(MAX) NULL,
# [preis_min] FLOAT NULL,
# [preis_avg] FLOAT NULL,
# [preis_max] FLOAT NULL,
# [avail] TINYINT NULL,
# [oe_vk] FLOAT NULL,
# [oe_nn] FLOAT NULL,
# [de_vk] FLOAT NULL,
# [de_nn] FLOAT NULL,
# [oe_kr] FLOAT NULL,
# [de_kr] FLOAT NULL,
# [anz_angebote] BIGINT NULL,
# [dtimebegin] BIGINT NULL,
# [dtimeend] BIGINT NULL

# ANGEBOT FILE PATHS SCHEME
# Where:
# {year} has always 4 digits
# {week} has always 2 digits
# Starting from 2006w16 up to 2023w43
# but we need only:
# w can go until 50, 51, 52, 53 (business year)
# w starts with 01
ANGEBOTE_SCHEME = 'angebot_{year}w{week}.parquet'


# CLICKS FOLDER
CLICKS_FOLDER = "clicks"
# [ip] NVARCHAR(MAX) NULL,
# [ip_country] NVARCHAR(MAX) NULL,
# [gh_land] NVARCHAR(MAX) NULL,
# [cookie] NVARCHAR(MAX) NULL,
# [produkt_id] BIGINT NULL,
# [haendler_bez] NVARCHAR(MAX) NULL,
# [dtime] NVARCHAR(MAX) NULL,
# [hloc_at] TINYINT NULL,
# [hloc_de] TINYINT NULL,
# [hloc_uk] TINYINT NULL,
# [hloc_pl] TINYINT NULL,
# [hloc_nl] TINYINT NULL,
# [hloc_ie] TINYINT NULL,
# [timestamp] BIGINT NULL

# CLICK FILE PATHS SCHEME INFO
# Where:
# {year} has always 4 digits (2006 to 2022)
# {month} has always 2 digits (from 01 to 12)
# Clicks begin with clicks_2006m04
CLICKS_SCHEME = 'clicks_{year}m{month}.parquet'

# RETAILERS FILES
RETAILERS = 'haendler.parquet'
# [haendler_bez] NVARCHAR(MAX) NULL,
# [is_at] TINYINT NULL,
# [is_de] TINYINT NULL,
# [is_uk] TINYINT NULL,
# [is_nl] TINYINT NULL,
# [is_pl] TINYINT NULL,
# [laden] TINYINT NULL,
# [abhol] TINYINT NULL,
# [online] TINYINT NULL,
# [lon] FLOAT NULL,
# [lat] FLOAT NULL,
# [versandk_default] NVARCHAR(MAX) NULL,
# [kunden_id] BIGINT NULL,
# [mastercard] TINYINT NULL,
# [visa] TINYINT NULL,
# [amex] TINYINT NULL,
# [dinersclub] TINYINT NULL,
# [vk_at] TINYINT NULL,
# [vk_de] TINYINT NULL,
# [nn_at] TINYINT NULL,
# [nn_de] TINYINT NULL,
# [liefert_at] TINYINT NULL,
# [liefert_de] TINYINT NULL,
# [liefert_uk] TINYINT NULL,
# [liefert_pl] TINYINT NULL,
# [liefert_nl] TINYINT NULL,
# [liefert_ie] TINYINT NULL,
# [dtimeBegin] BIGINT NULL,
# [dtimeEnd] FLOAT NULL

# PRODUCTS FILE
PRODUCTS = 'produkt.parquet'
# [produkt_id] BIGINT NULL,
# [produkt_bez] NVARCHAR(MAX) NULL,
# [subsubkat] BIGINT NULL,
# [dtime_birth] BIGINT NULL,
# [dtime_death] BIGINT NULL,
# [brand] NVARCHAR(MAX) NULL


# SCRAPER IPs (Contains 68 IP MD5 hashes)
SCRAPPER_IPS = 'scrapper_ips.parquet'
# [ip] NVARCHAR(MAX) NULL,
# [size] UINT NULL

# Versand (Shipping) Folders
VERSAND_11_15_FOLDER = "/versand_11_15"
VERSAND_06_10_FOLDER = "/versand_06_10"
VERSAND_FOLDER = "/versand"
# [angebot_id] BIGINT NULL,
# [oe_pp] FLOAT NULL,
# [de_pp] FLOAT NULL,
# [uk_vk] FLOAT NULL,
# [uk_kr] FLOAT NULL,
# [uk_nn] FLOAT NULL,
# [uk_pp] FLOAT NULL,
# [pl_vk] FLOAT NULL,
# [pl_kr] FLOAT NULL,
# [pl_nn] FLOAT NULL,
# [pl_pp] FLOAT NULL,
# [ie_vk] FLOAT NULL,
# [ie_nn] FLOAT NULL,
# [ie_kr] FLOAT NULL,
# [ie_pp] FLOAT NULL,
# [nl_vk] FLOAT NULL,
# [nl_nn] FLOAT NULL,
# [nl_kr] FLOAT NULL,
# [nl_pp] FLOAT NULL


# Verfügbarkeit (Availability) Folders
VERFUEGBARKEIT_11_15_FOLDER = "/verfuegbarkeit_11_15"
VERFUEGBARKEIT_FOLDER = "/verfuegbarkeit"
# [angebot_id] BIGINT NULL,
# [vfb_in_at] TINYINT NULL,
# [vfb_in_de] TINYINT NULL,
# [vfb_in_uk] TINYINT NULL,
# [vfb_in_pl] TINYINT NULL,
# [vfb_in_nl] TINYINT NULL,
# [vfb_in_ie] TINYINT NULL,
# [vfb_in_sk] TINYINT NULL,
# [vfb_in_ch] TINYINT NULL


# Product Specs Folders
PROD_SPECS_SSC_FOLDER = "/prod_specs_ssc"
PROD_SPECS_SC_FOLDER = "/prod_specs_sc"
PROD_SPECS_CAT_FOLDER = "/prod_specs_cat"

PRODUKT_SPECS_COUNT_FILE = "/produkt_specs_count.parquet"

# Lookups Folder
LOOKUPS_FOLDER = "/lookups"

# LCT Cluster Folder
LCT_CLUSTER_FOLDER = "/lct_cluster"
# [ip] NVARCHAR(MAX) NULL,
# [produkt_id] BIGINT NULL,
# [haendler_bez] NVARCHAR(MAX) NULL,
# [timestamp] BIGINT NULL,
# [cluster_produkt_id] FLOAT NULL,
# [lct_produkt_id] FLOAT NULL,
# [subsubkat] BIGINT NULL,
# [cluster_subsubkat] FLOAT NULL,
# [lct_subsubkat] FLOAT NULL,
# [subcat_id] FLOAT NULL,
# [cluster_subcat_id] FLOAT NULL,
# [lct_subcat_id] FLOAT NULL,
# [cat_id] INT NULL,
# [cluster_cat_id] FLOAT NULL,
# [lct_cat_id] FLOAT NULL


# Category Folder
KATEGORIE_FOLDER = "/kategorie"
# [cat_id] INT NULL,
# [cat] NVARCHAR(MAX) NULL,
# [dtime] BIGINT NULL

# [subcat_id] INT NULL,
# [subcat] NVARCHAR(MAX) NULL,
# [cat_id] INT NULL,
# [dtime] BIGINT NULL

# [ssc_id] NVARCHAR(MAX) NULL,
# [subsubcat] NVARCHAR(MAX) NULL,
# [subcat_id] INT NULL,
# [dtime] BIGINT NULL

# mapping:
# [ssc_id] NVARCHAR(MAX) NULL,
# [subsubcat] NVARCHAR(MAX) NULL,
# [subcat_id] INT NULL,
# [subcat] NVARCHAR(MAX) NULL,
# [cat_id] INT NULL,
# [cat] NVARCHAR(MAX) NULL


# Abfrage Folders
ABFRAGE_PRODUKT_BEW_FOLDER = "/abfrage_produkt_bew"
# [cookie] NVARCHAR(MAX) NULL,
# [ip] NVARCHAR(MAX) NULL,
# [dtime] DATETIME NULL,
# [produkt_id] BIGINT NULL,
# [plz] BIGINT NULL,
# [hloc_at] TINYINT NULL,
# [hloc_de] TINYINT NULL,
# [hloc_uk] TINYINT NULL,
# [hloc_pl] TINYINT NULL,
# [hloc_nl] TINYINT NULL,
# [hloc_ie] TINYINT NULL,
# [timestamp] BIGINT NULL

ABFRAGE_HAENDLER_BEW_FOLDER = "/abfrage_haendler_bew"
# [cookie] NVARCHAR(MAX) NULL,
# [ip] NVARCHAR(MAX) NULL,
# [dtime] DATETIME NULL,
# [kunden_id] BIGINT NULL,
# [plz] BIGINT NULL,
# [hloc_at] TINYINT NULL,
# [hloc_de] TINYINT NULL,
# [hloc_uk] TINYINT NULL,
# [hloc_pl] TINYINT NULL,
# [hloc_nl] TINYINT NULL,
# [hloc_ie] TINYINT NULL,
# [timestamp] BIGINT NULL

ABFRAGE_FILTER_FOLDER = "/abfrage_filter"
# [cookie] NVARCHAR(MAX) NULL,
# [ip] NVARCHAR(MAX) NULL,
# [dtime] DATETIME NULL,
# [produkt_id] BIGINT NULL,
# [angebote] NVARCHAR(MAX) NULL,
# [verfuegbarkeit] NVARCHAR(MAX) NULL,
# [merken] TINYINT NULL,
# [plz] BIGINT NULL,
# [hloc_at] TINYINT NULL,
# [hloc_de] TINYINT NULL,
# [hloc_uk] TINYINT NULL,
# [hloc_pl] TINYINT NULL,
# [hloc_nl] TINYINT NULL,
# [hloc_ie] TINYINT NULL,
# [timestamp] BIGINT NULL


SSC_SC_CATS_FILE = "/ssc_sc_cats.parquet"
# [ghid] BIGINT NULL,
# [title] NVARCHAR(MAX) NULL,
# [category] NVARCHAR(MAX) NULL,
# [gtins] NVARCHAR(MAX) NULL,
# [description] NVARCHAR(MAX) NULL,
# [rating] FLOAT NULL,
# [rating_count] FLOAT NULL,
# [rating_info] NVARCHAR(MAX) NULL,
# [image_url] NVARCHAR(MAX) NULL,
# [image_thumbnail_url] NVARCHAR(MAX) NULL,
# [best_price] FLOAT NULL,
# [average_price] FLOAT NULL,
# [median_price] FLOAT NULL,
# [specs] NVARCHAR(MAX) NULL,
# [Typ] NVARCHAR(MAX) NULL,
# [Besonderheiten] NVARCHAR(MAX) NULL,
# [Gelistet_seit] NVARCHAR(MAX) NULL


PRODUKTBEWERTUNG_FILE = "/produktbewertung.parquet"
# [ip] NVARCHAR(MAX) NULL,
# [user_id] NVARCHAR(MAX) NULL,
# [cookie] NVARCHAR(MAX) NULL,
# [produkt_bew_id] BIGINT NULL,
# [produkt_id] BIGINT NULL,
# [empfehlung] TINYINT NULL,
# [features] TINYINT NULL,
# [value] TINYINT NULL,
# [quality] TINYINT NULL,
# [support] TINYINT NULL,
# [status] TINYINT NULL,
# [dtime] DATETIME NULL,
# [timestamp] BIGINT NULL


MARKEN_FILE = "/marken.parquet"
# [brand] NVARCHAR(MAX) NULL,
# [no_products] BIGINT NULL

PRODUKT_MARKEN_FILE = "/produkt_marken.parquet"
#

HAENDLERBEWERTUNG_FILE = "/haendlerbewertung.parquet"
# [haendler_bew_id] BIGINT NULL,
# [kunden_id] BIGINT NULL,
# [ip] NVARCHAR(MAX) NULL,
# [dtime] DATETIME NULL,
# [user_id] NVARCHAR(MAX) NULL,
# [a1] TINYINT NULL,
# [a2] TINYINT NULL,
# [a3] TINYINT NULL,
# [a4] TINYINT NULL,
# [a5] TINYINT NULL,
# [a6] TINYINT NULL,
# [a7] TINYINT NULL,
# [a8] TINYINT NULL,
# [a9] TINYINT NULL,
# [b1] TINYINT NULL,
# [b2] TINYINT NULL,
# [b3] TINYINT NULL,
# [b4] TINYINT NULL,
# [b5] TINYINT NULL,
# [b6] TINYINT NULL,
# [shop] FLOAT NULL,
# [versand] FLOAT NULL,
# [lagerstand] FLOAT NULL,
# [bestellstatus] FLOAT NULL,
# [kundenservice] FLOAT NULL,
# [gesamtbewertung] FLOAT NULL,
# [valid] TINYINT NULL,
# [invalid_ts] BIGINT NULL,
# [timestamp] BIGINT NULL

DAILY_HBEW_FILE = "/daily_hbew.parquet"
#

CONTINUING_OFFERS_FILE = "/continuing_offers.parquet"
#

# tbc.
