from pathlib import Path

# DEFINITION OF GLOBAL CONSTANTS #

# Processors
SPAWN_MAX_MAIN_PROCESSES_AMOUNT = 10

# Loaders
OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED = 52
OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED = 26

# Sampler
RANDOM_SAMPLER_DETERMINISTIC_SEED = 42

# Sampler Params
RANDOM_PRODUCTS_AMOUNTS = 50
RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT = 10  # + 1 from the firm looking

# Params
MAX_TIME_WINDOW_WEEKS_AROUND_SEAL_WEEKS_AMOUNT = 52

# n ~ firms x 50 x (10+1) x 52

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

# UNIX Time Constants
UNIX_HOUR = 60 * 60
UNIX_DAY = 60 * 60 * 24
UNIX_WEEK = 7 * 24 * 60 * 60  # 604800
UNIX_MONTH = 2629743  # 1 Month (30.44 days), see https://www.unixtimestamp.com/ for details
UNIX_YEAR = 31556926  # 1 Year (365.24 days), see https://www.unixtimestamp.com/ for details

UNIX_WEDNESDAY_MIDDAY_INTERCEPT = UNIX_DAY * 2.5  # From Monday 0:00 GMT+0000

# Forbidden Retailer Keywords
FORBIDDEN_RETAILER_KEYWORDS = [
    '-am-uk', '-am-de', '-am-at', '-eb-uk', '-eb-de', '-sh-at', '-mp-de',
    '-rk-de', '-nk-pl', '-sz-uk', '-vk-de', '-gx-de'
]

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

# Analysis Parameters
TOP_PRODUCTS_OF_SEAL_CHANGE_FIRM_BY_CLICKS_AMOUNT = 200  # Top 200 products i of seal change firm
HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT = 8  # 8 weeks before and 8 weeks after
MAX_WEEKS_BEFORE_AFTER_PRODUCT_ANGEBOTEN_MISSING_ALLOWED_AMOUNT = 1  # 1 week missing is allowed
MAX_DAYS_ANGEBOT_MISSING_WITHIN_WEEK = 0  # 0 = "Durchgehend angeboten", product must have an offer each day in a week

# FILES
CSV_DELIM_STYLE = ","

# GLOBAL FILE PATHS
# PARQUE_FILES_DIR = Path("/nfn_vwl/geizhals/zieg_pq_db")
PARQUE_FILES_DIR = Path("../data")

# SINGLE FILE PATHS

# FILTERED HAENDLER BEZ (see project FR-01, refer to filtered_haendler_bez.csv)
FILTERED_HAENDLER_BEZ = '../data/filtered_haendler_bez.csv'  # Contains j ∈ { J_G, J_C }

# SEAL CHANGE FIRMS J_G, seal change data t_sealchange, and seal provider names
SEAL_CHANGE_FIRMS = '../data/final_matrix.csv'  # Contains j ∈ { J_G } in the column RESULTING MATCH
# and t_seal_change in the column Guetesiegel First Date in the format of 12.07.2007 until row 255
# the seal provider is in the first column

# ANGEBOTE FOLDER
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

# CLICKS FOLDER
CLICKS_FOLDER = "clicks"

# SINGLE FILES

# ANGEBOT FILE PATHS SCHEME
# Where:
# {year} has always 4 digits
# {week} has always 2 digits

# Starting from 2006w16 up to 2023w43
# but we need only:
# w can go until 50, 51, 52, 53 (business year)
# w starts with 01
ANGEBOTE_SCHEME = 'angebot_{year}w{week}.parquet'

# CLICK FILE PATHS SCHEME INFO
# Where:
# {year} has always 4 digits (2006 to 2022)
# {month} has always 2 digits (from 01 to 12)
# Clicks begin with clicks_2006m04
CLICKS_SCHEME = 'clicks_{year}m{month}.parquet'
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

# RETAILERS FILES
RETAILERS = 'haendler.parquet'

# PRODUCTS FILE
PRODUCTS = 'produkt.parquet'

# SCRAPER IPs (Contains 68 IP MD5 hashes)
SCRAPPER_IPS = 'scrapper_ips.parquet'
