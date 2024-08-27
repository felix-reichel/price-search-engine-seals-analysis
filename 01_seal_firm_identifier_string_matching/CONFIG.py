# CONFIG.py

# GLOBAL CONSTANTS
ALLOW_SKIPPING = True
CSV_SEPARATOR = ";"

# Input and Output Folders
INPUT_FOLDER = "input/"
OUTPUT_FOLDER = "output/"

# Step 0 - Retailer filtering
GEIZHALS_RETAILERS_PARQUE_FILE_PATH = INPUT_FOLDER + "haendler.parquet"
FILTERED_RETAILERS_CSV_FILE_PATH = OUTPUT_FOLDER + "filtered_haendler_bez.csv"

FORBIDDEN_RETAILER_KEYWORDS = ['am-uk', 'am-de', 'am-at', 'eb-uk', 'eb-de', 'sh-at', 'mp-de', 'rk-de', 'nk-pl', 'sz-uk',
                               'vk-de', 'gx-de']  # Amazon UK, DE, AT retailers


# Step 1 - Retailer matching (Matching of Geizhals_bez and guetesiegel retailer name)
# Input files
ECOMMERCE_GUETESIEGEL_FILE_PATH = INPUT_FOLDER + "e_commerce.csv"
EHI_BEVH_GUETESIEGEL_FILE_PATH = INPUT_FOLDER + "ehi_bevh.csv"
HANDELSVERBAND_GUETESIEGEL_FILE_PATH = INPUT_FOLDER + "handelsverband.csv"

# Strip prefixes and suffixes
GUETESIEGEL_NAME_PREFIXES = ['https://', 'http://', 'eshop.', 'shop.', 'www.', 'shop.']
GUETESIEGEL_NAME_SUFFIXES = ['/shop', '/de-AT', '/webshop', '/marktplatz', '/onlineshop', '/at', '/at/shop', '/de',
                             '/george']

# Matching Constants
INSUFFICIENT_RETAILER_MATCH_SUFFIXES = ['-at', '-uk', '-de', '-com']

# Output paths
ECOMMERCE_MATCHED_FILE_PATH = OUTPUT_FOLDER + "e-commerce_matched.csv"
EHI_BEVH_MATCHED_FILE_PATH = OUTPUT_FOLDER + "ehi_bevh_matched.csv"
HANDELSVERBAND_MATCHED_FILE_PATH = OUTPUT_FOLDER + "handelsverband_matched.csv"

# Other Constants - HEADER COLUMNS
HEADER_COLUMN_NAME_ADVANCED_MATCHING_1 = "first or last 5 chars or 3 chars if len <= 5 (without '-suffixes')"
HEADER_COLUMN_NAME_ADVANCED_MATCHING_2 = ("first or last 3 chars or 4 chars if 'Dot' Index long enough (without "
                                          "'-suffixes')")

HEADER_COLUMN_NAME_ADVANCED_MATCHING_JARO = "JARO Top-3"  # "Uses the Jaro similarity index and lists the 3 highest
# candidates"
HEADER_COLUMN_NAME_ADVANCED_MATCHING_JARO_TOP_1 = "JARO Top-1"
HEADER_COLUMN_RESULTING_MATCH = "RESULTING MATCH"

# Step 2 - Post-Matching Processing

# STEP_2_FILE_SUFFIX = "_reviewed"
STEP_2_FILE_SUFFIX = "_matched_reviewed_franz_wrong_col_minus_one"
HEADER_COLUMN_RESULTING_MATCH_REVIEWED_FRANZ_WRONG_COL_NAME = "matching_criteria_simple"
EXCLUDE_MATCH_NO_CANDIDATE_FRANZ = "-"
CSV_FILE_SUFFIX = ".csv"

# Input files
ECOMMERCE_GUETESIEGEL_REVIEWED_FILE_PATH = INPUT_FOLDER + "e_commerce" + STEP_2_FILE_SUFFIX + CSV_FILE_SUFFIX
EHI_BEVH_GUETESIEGEL_REVIEWED_FILE_PATH = INPUT_FOLDER + "ehi_bevh" + STEP_2_FILE_SUFFIX + CSV_FILE_SUFFIX
HANDELSVERBAND_GUETESIEGEL_REVIEWED_FILE_PATH = INPUT_FOLDER + "handelsverband" + STEP_2_FILE_SUFFIX + CSV_FILE_SUFFIX

# Step 3 - Descriptive Statistics of "reviewed" and finally merged retailers

# Resulting matrice column merged on col names
HANDELSVERBAND_DATE_FROM_COL_NAME = "year"
E_COMMERCE_DATE_FROM_COL_NAME = "Zertifiziert seit"
EHI_BHV_DATE_FROM_COL_NAME = "eval_since"

FINAL_MATRIX_FILENAME = 'final_matrix.csv'
FINAL_MATRIX_FILE_PATH = OUTPUT_FOLDER + FINAL_MATRIX_FILENAME

# Step 4 - Count Statistics of matched, reviewed, and merged retailers
COUNT_MATRIX_FILENAME = 'count_matrix.csv'
COUNT_MATRIX_FILE_PATH = OUTPUT_FOLDER + COUNT_MATRIX_FILENAME

COLUMN_DATE = 'guetesiegel_first_date'
COLUMN_FILENAMECP = 'filenamecp'
COLUMN_YEAR = 'year'
COLUMN_IS_AT = 'is_at'
COLUMN_IS_DE = 'is_de'
