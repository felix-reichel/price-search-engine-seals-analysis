# FR-03 Geizhals Quality Seal Retailer Data Set

**Author**: FR  
**Date**: June 2024 - ?2024

## Data Set Description & Requirements

### Observational Unit:
The observational unit is defined as **i, j, t**, where:
- **i**: product (`product_id`)
- **j**: company (`geizhals_firm_id`)
- **t**: week (counted from UNIX time, starting from the defined UNIX TIME ORIGIN).

### Period \(T\):
The observation period covers a maximum of 26 weeks before and 26 weeks after (up to 52 weeks total) the quality seal award date, while considering only products with a new offer spell from 52 weeks before the seal date (Offer Spells Inflow).

### Companies \(J\):
Includes all companies except marketplace companies (e.g., Amazon, Shöpping). Companies with the following terms in their names are excluded:
```['-am-uk', '-am-de', '-am-at', '-eb-uk', '-eb-de', '-sh-at', '-mp-de', '-rk-de', '-nk-pl', '-sz-uk', '-vk-de', '-gx-de']```

### Products \(I\):
Includes all products, with selection criteria as described below.

### Unbalanced Sample:
If products or companies are not available during the maximum observation period (26*2=52 weeks), those observations will be missing. ("panel attrition")

## Observational Unit Selection Criteria

### Step 1: Firms with Seal Status Change
We include all firms (**j**) that experienced a "change in seal status" (tracked by 3 seal dummies). These firms are awarded a seal at week **t**. Firms may possess multiple seals (up to 3 seal dummies = \(2^3\) combinations of binary variables). Quality Seal award dates were obtained from web-scraping see project hb-07 (e.g., Handelsverband).

### Step 2: Select 50 Products per Firm
For each firm (**j**), we select 50 products (**i**) based on:
1. Random sampling of 50 products. (to draw from a normal)
2. Continuous offering by the firm at least \(W\) weeks before and \(W\) weeks after the seal award (with allowance for 1 missing week. DEFINITION: No offer spell in the week exists.).

### Step 3: Counterfactual Firms
We identify other firms offering product **i** at the time of the seal award. A maximum of 10 firms are randomly selected. If fewer than 10 firms offer the product, all are selected.

### Dataset Dimensions:
The dataset size is based on the firms \(J_G + J_C\) (seal and counterfactual firms), products (\(I_j_g = 50\)), and time periods (up to 26*2 weeks).

## Data Set Specifications & Requirements
For detailed data set specifications, refer to:  
`Spezifikation_DS_guetesiegel_{VERSION}.doc`

## Pre-Checks of Retailer Data

### Check 1: Availability of liefert_at and liefert_de Variables
Check if the information (`liefert_at/liefert_de`) is available before 2007.  
**Result**: These variables have many missing values before 2015. Imputation strategies are proposed.

### Check 2: Availability of vfb_at Variable
Check if `vfb_at` was available before 2007 in the "Verfügbarkeit" data.  
**Result**: Parquet files are available starting from 2015. Imputation strategies are outlined in the documentation.

## Data Set Implementation

### Step 1: Populate Running Variables \(i, j, t\)
- Ensure distinct products (**i**) and firms (**j**) are plausible.
- Verify the number of rows matches expectations.

### Step 2: Merge Running Variables with Existing Data
Merge the populated variables with seal dummies and award dates. Ensure consistency through plausibility checks.

### Step 3: Calculation of Variables
Calculate necessary variables and instrumental variables based on the dataset.

### Imputation Strategy:
Step-wise imputation strategies are applied based on the specific requirements.

## Execution
To execute, run `main.py` for Step 1.

## Estimated Dataset Size
The maximum number of observations is estimated based on combinations of firms, products, and weeks:

\[
296 \, (\text{seals matrix}) \times 52 \, (\text{max. window}) \times 50 \, (\text{products}) \times (10 + 1) \, (\text{counterfactuals}) \approx 8.45 \, \text{million observations}
\]

Where:
- **j**: retailer (`haendler_bez`)
- **i**: product ID
- **t**: week

## Data Set Plausibility Tests
After populating the running variables, check if the number of rows is within the expected range.

## Tests
The following tests are implemented:

- General Tests:
  - `TestConfig.py`
  - `*.py`

- Selection Criteria Tests:
  - `TestFilterContinouslyOfferedProducts.py`
  - `TestGetRandMaxNCounterfactualFirms.py`
  - `TestGetStartOfWeek.py`
  - `TestGetTopNProductsbyClicks.py`
  - `TestIsProductContinouslyOffered.py`

## Configuration Details (CONFIG.py)

### Processors
- **SPAWN_MAX_MAIN_PROCESSES_AMOUNT**: 10

### Loaders
- **OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED**: 52 weeks
- **OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED**: 26 weeks

### Sampler
- **RANDOM_SAMPLER_DETERMINISTIC_SEED**: 42
- **RANDOM_PRODUCTS_AMOUNTS**: 50
- **RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT**: 10

### UNIX Time Origin
- **UNIX_TIME_ORIGIN**: 1179093600 (Mon May 14, 2007)
- **UNIX_TIME_COLLAPSE**: 1703977200 (Sun Dec 31, 2023)

### Global File Paths
- **PARQUE_FILES_DIR**: `../data`

### Specific Files
- **FILTERED_HAENDLER_BEZ**: `../data/filtered_haendler_bez.csv`
- **SEAL_CHANGE_FIRMS**: `../data/final_matrix.csv`
- **ANGEBOTE_FOLDERS**: 
  - `angebot_06_10`
  - `angebot_11_15`
  - `angebot`

- **CLICKS_FOLDER**: `clicks`
- **RETAILERS**: `haendler.parquet`
- **PRODUCTS**: `produkt.parquet`
- **SCRAPER_IPS**: `scrapper_ips.parquet`
