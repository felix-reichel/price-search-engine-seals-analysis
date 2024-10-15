# FR-03 Geizhals Quality Seal Retailer Data Set

**Author**: FR  
**Date**: June 2024 - ?2024

## Data Set Specifications & Requirements

Approximate variables amount (columns) is: 90 Vars. + 4 IVs.

For original data set specifications (requirements), refer to:  
`Spezifikation_DS_guetesiegel_{VERSION}.doc`

### Requirements Engineering: Data Set Columns Overview

For a detailed overview of the calculated variables refer to:

[spec > REQUIREMENTS_ENGINEERING > DATASET](spec/REQUIREMENTS_ENGINEERING/DATASET.md)



## Observational Unit:
The observational unit $\text{obs}(i, j, t)$ is defined as:
- **$i$**: product (`product_id`) where $i \in \mathbb{Z}^+$ (is at least a positive integer number).
- **$j$**: company (`geizhals_firm_id`) where $j \in \Sigma^*$ (is is at least any valid combination of characters).
- **$t$**: week (calculated running variable from UNIX time, starting from the defined UNIX TIME ORIGIN or formally given as:

$$
t =
\begin{cases}
\left\lfloor \dfrac{u - u_0}{604800} \right\rfloor, & \text{if } -26 \leq \left\lfloor \dfrac{u - u_0}{604800} \right\rfloor \leq 832 \\
\text{undefined}, & \text{otherwise}
\end{cases}
$$

 where $\lfloor \rfloor$ denotes the floor function.

### $t \in$ Period $T$ ($t \in T_{\text{max length truncated symmetric (i,j)}} \subseteq T_{\text{inflow}}$):
The observation period covers a **truncated**, **symmetric** window of 26 weeks before and 26 weeks after ($t \leq 52$ weeks) the quality seal award date. Only products with a new offer spell from 52 weeks before the seal date and/or 26 weeks after the seal date are considered ($t \leq 78$ weeks, "Offer Spells Inflow").

$$
T_{\text{inflow}} = \{ t \mid -52 \leq t - t_{\text{seal}} \leq 26 \}
$$

$\implies$ *Products outside the "Offer Spells Inflow" (e.g., those with no frequent price adjustments or whose life cycle is outside this inflow period) are disregarded* $\implies$ *(Possible source of) Selection bias, Endogeneity.*

### $j \in$ Companies $J$ ($j \in J_{\text{seal}} \cup J_{\text{cf}}$ or $j \in J_{\text{seal + counterfactual}} \subseteq J_{\text{filtered}}$):
The set of companies includes all firms $j \in J$ except marketplace companies. Firms with the following terms in their names are excluded:

$$
J_{\text{filtered}} = J \setminus \{ j \mid \text{name contains excluded substrings: } ['-am-uk', '-am-de', '-am-at', '-eb-uk', '-eb-de', '-sh-at', '-mp-de', '-rk-de', '-nk-pl', '-sz-uk', '-vk-de', '-gx-de'] \}
$$

This results in 18k+ firms from 3 million total retailers. Ensure the newest, correctly filtered retailers are used, excluding those matching the substrings listed above.

### $i \in$ Products $I$ (Selection of Top N and Offered Products)

The set of products includes all products $i$ that are selected based on the criteria and offered by firms $j \in J_{\text{seal}}$ during the period $T_{\text{max length truncated symmetric}}$.

$$
i \in I_{\text{top N}} \cup I_{\text{offered}} \subseteq I_{\text{selected}}
$$

Where:

$$
I_{\text{selected}} = \{ i \mid i \in I_{\text{offered}} \cup I_{\text{top N}} \text{ and } i \text{ is offered by } j \in J_{\text{seal}} \text{ during } T_{\text{max length truncated symmetric}} \}
$$

### $(i, j, t)$ Unbalanced Sample:
If products or companies are not available during the maximum observation period ($26*2 = 52$ weeks), those observations will be missing ("panel attrition"). 

$\implies$ *Results graphically in wider CIs from the standardized seal award date.*

## Observational Unit Selection Criteria

### Step 1: Firms with Seal Status Change

Let $J_{\text{seal}} \subseteq J$ represent the set of all firms $j$ that experienced a change in seal status, where the change is tracked by three binary seal dummies. (up to 3 seal dummies = $2^3$ combinations of binary variables). Quality seal award dates were obtained from web scraping, see project hb-07 (e.g., Handelsverband).

$$
J_{\text{seal}} = \{ j \in J \mid \exists \, t \in T, \, \text{SealChange}(j, t) = 1 \}
$$

Where:
- $\text{SealChange}(j, t)$ is a function returning 1 if firm $j$ experiences a seal status change at time $t$.

### Step 2: Select Top N Products per Firm

Let $I_j$ represent the set of products offered by firm $j$. The **Top N products** ($N = 200$) for each firm $j \in J_{\text{seal}}$ are selected based on the following criteria:

#### 1. **Considered Product Criteria**: Top N by Total Clicks

Let $C(i, t)$ be the total clicks for product $i$ aggregated over a period of 6 months before and 6 months after the seal award date $t$.

$$
I_{\text{top N}} = \{ i \in I_j \mid \sum_{t' = t-26}^{t+26} C(i, t') \text{ is among the top N for } j \}
$$

Where:
- $C(i, t')$ is the number of clicks for product $i$ in week $t'$.
- $t-26$ and $t+26$ represent the 6-month window before and after the seal award date.

#### 2. **Offered Product Criteria**: Continuous Offering

The products must be offered continuously by the firm $j$ for at least $W$ weeks before and after the seal award, allowing for a maximum of one missing week. This condition can be formalized as:

$$
I_{\text{offered}} = \{ i \in I_{\text{top N}} \mid \sum_{t' = t-W}^{t+W} \mathbb{1}(\text{Offered}(j, i, t')) \geq 2W - 1 \}
$$

Where:
- $\mathbb{1}(\text{Offered}(j, i, t'))$ is an indicator function that returns 1 if product $i$ is offered by firm $j$ at time $t'$, and 0 otherwise.
- The total sum must be at least $2W - 1$ to allow for one missing week.

### Step 3: Counterfactual Firms

For each selected product $i \in I_{\text{offered}}$ of firm $j \in J_{\text{seal}}$, a set of counterfactual firms $J_{\text{cf}}(i, j) \subseteq J$ is selected, which consists of other firms offering the same product $i$ at the time of the seal award. A maximum of 10 firms is deterministically, randomly sampled:

$$
J_{\text{cf}}(i) = \{ j' \in J \mid \text{Offered}(j', i, t) = 1 \text{ and } j' \neq j \}
$$

Where:
- If $|J_{\text{cf}}(i)| \leq 10$, all firms are selected. Otherwise, 10 firms are selected randomly.

### Dataset Dimensions:
The dataset size is based on the firms $J_G + J_C$ (seal and counterfactual firms), products ($I_{g+c}$), and **truncated**, **symmetric** time periods (up to $T_{maxLength}=26*2$ weeks).

## Pre-Checks of Retailer Data

### Check 1: Availability of `liefert_at` and `liefert_de` Variables
Check if the information (`liefert_at/liefert_de`) is available before 2007.  
**Result**: These variables have many missing values before 2015. Imputation strategies are proposed.

### Check 2: Availability of `vfb_at` Variable
Check if `vfb_at` was available before 2007 in the "Verfügbarkeit" data.  
**Result**: Parquet files are available starting from 2015. Imputation strategies are outlined in the documentation.

## Data Set Implementation / Explaination of Scripts 

### Script 0: Initialize an In-Memory DuckDB Database (C++) using its Python Client API
- Initialize `DuckDBDataSource` and verify the presence of required tables: `seal_change_firms`, `filtered_haendler_bez`, `products`, `retailers`. Log the row counts for each table.

### Script 1: Initialize Global Data Set / Quality Seal Retailers Parameters using Results from Previous Projects `Fr-01` and `Fr-02`
- Fetch `allowed_firms` from `filtered_retailer_names_repo` and `seal_firms` from `seal_change_firms_repo`.

### Script 2: Calculate Observational Unit Selection Criteria $(i,j,t)$-Index Space
- Run `calculate_index_space(parallel=False)` to compute the $(i, j, t)$ index space.

### *Script 2b: Calculate Affected Products Never Considered Due to the Offer Inflow Loading Strategy
- Check whether this bias is uniform across products.

### Script 3: Load Observational Unit Selection Criteria $(i,j,t)$-Index Space into a Table
- Load the calculated index space table into the database (from `results.csv`).

### Script 4: Validate Index Space and Produce Descriptive Stats.
- Generate descriptive statistics:
  - Distribution of products selected per seal firm.
  - Average number of counterfactual firms per product.
  - Total number of products in the dataset.
  - Average observation period length (2 * 26 weeks).
- Ensure the statistics align with the *estimated dataset size* and pass further plausibility checks.

### *Project 4: Graphical Representation of $(i,j,t)$-Index Space

### *Project X: Formal Observational Unit Selection Criteria Bias

### Script 5: Insert New Columns from Existing Repositories
- Insert columns for seal dummies, award dates, and other relevant data from repositories.

### Script 6: Calculate Variables (using BatchVariableRenderer)
- Calculate necessary variables for the data set.

### *Project 5: Graphical PTA Check for DD After Processing of the First Outcome Variable

### Script 6b: Calculate IVs Based on Specific Strategies (using BatchVariableRenderer)
- Calculate instrumental variables (IVs) based on specific strategies.

### Script 7: Output New Preliminary Data Set as `'_preliminary'`-CSV
- Save the preliminary dataset to a CSV file and copy it to a secondary table for further processing.

### Script 8: Imputation of Variables (using ImputationService)
- Use the imputation service to handle missing values through stepwise imputation (refer to `ImputationStrategy`) as per dataset requirements.

### Script 9: Output Final Data Set as `'_imputed'`-CSV
- Output the final dataset to a CSV file.

## Max. Estimated Dataset Size
The **maximum** number of observations is estimated based on combinations of firms, products, and weeks:

$$
296 \ (\text{seals matrix})
$$

$$
\times 52 \ (\text{max. window from "offer inflow"; bias check script 2b}) 
$$

$$
\times 200 \ (\text{Top N=200 products by clicks; project X bias from selction strategy}) 
$$

$$
\times (10 + 1) \ (\text{counterfactuals + seal change firm; random sampling; distributional check script 4}) 
$$

$$
\approx 33.86 \ \text{million observations}
$$

Where:
- **$j$**: retailer (`haendler_bez`)
- **$i$**: product ID
- **$t$**: week

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

## Configuration Details (`CONFIG.py`)

### Threads Config
- **MAX_DUCKDB_THREADS**: 32
- **MAX_DUCKDB_BACKGROUND_THREADS**: 2
- **POLARS_MAX_THREADS**: 32

### Multiprocessing Config
- **SPAWN_MAX_MAIN_PROCESSES_AMOUNT:** 8
 
### Loaders
- **OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_PRE_SEAL_CONSIDERED**: 52 weeks (1 year)
- **OFFER_TIME_SPELLS_PREPROCESSING_WEEKS_POST_SEAL_CONSIDERED**: 26 weeks (6 months)

### UNIX Time Constants
- **UNIX_HOUR**: 3600 seconds
- **UNIX_DAY**: 86400 seconds
- **UNIX_WEEK**: 604800 seconds
- **UNIX_MONTH**: 2629743 seconds (approx.)
- **UNIX_YEAR**: 31556926 seconds (approx.)
- **UNIX_WEDNESDAY_MIDDAY_INTERCEPT**: 2.5 days after UNIX start of the week
- **UNIX_TIME_ORIGIN**: May 14, 2007 (1179093600)
- **UNIX_TIME_COLLAPSE**: December 31, 2023 (1703977200)

### Sampler
- **RANDOM_SAMPLER_DETERMINISTIC_SEED**: 42
- **RANDOM_COUNTERFACTUAL_FIRMS_AMOUNT**: 10 firms (max)

### Observational Parameters
- **MAX_TIME_WINDOW_WEEKS_AROUND_SEAL_WEEKS_AMOUNT**: 52 weeks
- **TOP_PRODUCTS_OF_SEAL_CHANGE_FIRM_BY_CLICKS_AMOUNT**: 200 products
- **HAS_WEEKS_BEFORE_AND_AFTER_PRODUCT_ANGEBOTEN_AMOUNT**: Minimum of 4 weeks before and after the seal
- **MAX_WEEKS_BEFORE_AFTER_PRODUCT_ANGEBOTEN_MISSING_ALLOWED_AMOUNT**: 1 missing week allowed
- **MAX_DAYS_ANGEBOT_MISSING_WITHIN_WEEK**: 0 days missing allowed

### CSV Settings
- **CSV_IMPORT_DELIM_STYLE**: `;`
- **CSV_OUTPUT_DELIM_STYLE**: `,`

### Static Input Files
- **FILTERED_HAENDLER_BEZ**: `../data/filtered_haendler_bez.csv`
- **SEAL_CHANGE_FIRMS**: `../data/final_matrix.csv`

### Parquet Files and Folders
- **PARQUET_FILES_DIR**: Path to parquet files directory
- **ANGEBOTE_FOLDER**: Folders for product offers:
  - `angebot_06_10`
  - `angebot_11_15`
  - `angebot`
- **VERSAND_FOLDER**: Shipping cost data folder:
  - `versand_06_10`
  - `versand_11_15`
  - `versand`
- **VERFUEGBARKEIT_FOLDER**: Availability data folder:
  - `verfuegbarkeit_06_10`
  - `verfuegbarkeit_11_15`
  - `verfuegbarkeit`
- **CLICKS_FOLDER**: Folder for click data:
  - `clicks/clicks_<YYYY>m<MM>.parquet`
- **LCT_CLUSTER_FOLDER**: Folder for LCT clusters:
  - `lct_cluster/lct_cluster_<YYYY>m<MM>.parquet`
- **RETAILERS_FILE**: `haendler.parquet`
- **SCRAPER_IPS_FILE**: `scrapper_ips.parquet`
- **PRODUCTS_FILE**: `produkt.parquet`
- **MARKEN_FILE**: `marken.parquet`
- **PRODUCT_RATINGS_FILE**: `produktbewertung.parquet`
- **HAENDLERBEWERTUNG_FILE**: `haendlerbewertung.parquet`
- **DAILY_HBEW_FILE**: `daily_hbew.parquet`
- **CONTINUING_OFFERS_FILE**: `continuing_offers.parquet`
- **PRODUCT_SPECS_FOLDERS**: Folders for product specifications:
  - `prod_specs_ssc`
  - `prod_specs_sc`
  - `prod_specs_cat`
- **SSC_SC_CATS_FILE**: `ssc_sc_cats.parquet`
- **LOOKUPS_FOLDER**: `lookups`
- **ABFRAGE_PRODUKT_BEW_FOLDER**: `abfrage_produkt_bew`
- **ABFRAGE_HAENDLER_BEW_FOLDER**: `abfrage_haendler_bew`
- **ABFRAGE_FILTER_FOLDER**: `abfrage_filter`
- **CATEGORY_FILES**: 
  - `categories.parquet`
  - `subcats.parquet`
  - `subsubcats.parquet`
  - `ssc_sc_cats.parquet`


...

## Further Configuration Details
- `ApplicationThreadConfig.py`
- `SCHEMA_CONFIG.py`
- `TABLES_CONFIG.py`
- ...


