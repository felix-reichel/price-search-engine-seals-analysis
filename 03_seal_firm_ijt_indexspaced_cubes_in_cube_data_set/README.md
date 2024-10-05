# FR-03 Geizhals Quality Seal Retailer Data Set

**Author**: FR  
**Date**: June 2024 - ?2024

## Data Set Description & Requirements

### Observational Unit:
The observational unit $\text{obs}(i, j, t)$ is defined as:
- **$i$**: product (`product_id`)
- **$j$**: company (`geizhals_firm_id`)
- **$t$**: week (calculated running variable from UNIX time, starting from the defined UNIX TIME ORIGIN).

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

## Data Set Specifications & Requirements
For detailed data set specifications, refer to:  
`Spezifikation_DS_guetesiegel_{VERSION}.doc`

## Pre-Checks of Retailer Data

### Check 1: Availability of `liefert_at` and `liefert_de` Variables
Check if the information (`liefert_at/liefert_de`) is available before 2007.  
**Result**: These variables have many missing values before 2015. Imputation strategies are proposed.

### Check 2: Availability of `vfb_at` Variable
Check if `vfb_at` was available before 2007 in the "Verfügbarkeit" data.  
**Result**: Parquet files are available starting from 2015. Imputation strategies are outlined in the documentation.

## Data Set Implementation

### Step 0: Initialize Database
- Initialize `DuckDBDataSource` and verify required tables: `seal_change_firms`, `filtered_haendler_bez`, `products`, `retailers`. Log row counts for each table.

### Step 1: Initialize Global Seal Parameters
- Fetch `allowed_firms` from `filtered_retailer_names_repo` and `seal_firms` from `seal_change_firms_repo`.

### Step 2: Calculate Index Space
- Run `calculate_index_space(parallel=False)` to compute $(i, j, t)$.

### Step 3: Load Index Space
- Load the calculated index space table into the database. *(from results.csv)*

### Step 4: Validate Index Space
- Generate descriptive statistics:
  - Distribution of products selected per seal firm.
  - Average number of counterfactual firms per product.
  - Total number of products in the dataset.
  - Average observation period length (2 * 26 weeks).
- Ensure it aligns with the *estimated dataset size* below and further plausibility checks.

### Step 5: Insert New Columns
- Insert columns for seal dummies, award dates, and other relevant data from repositories.

### Step 6: Calculate Vars and IVs
- Calculate variables and instrumental variables.

### Step 7: Output New Preliminary Data Set
- Save the preliminary dataset to a file and copy it to a secondary table for further processing.

### Step 8: Imputation of Variables
- Use the imputation service to handle missing values through stepwise imputation (see ImputationStrategy) as per dataset requirements.

### Step 9: Output Final Data Set
- Output the final dataset to a CSV-file.

## Estimated Dataset Size
The maximum number of observations is estimated based on combinations of firms, products, and weeks:

$$
296 \ (\text{seals matrix})
\times 52 \ (\text{max. window from "offer inflow"}) 
\times 50 \ (\text{products}) 
\times (10 + 1) \ (\text{counterfactuals + seal change firm}) 
\approx 8.45 \ \text{million observations}
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
- **PARQUET_FILES_DIR**: `../data`

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

...
