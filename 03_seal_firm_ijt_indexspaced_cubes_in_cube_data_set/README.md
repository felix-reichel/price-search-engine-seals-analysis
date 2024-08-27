# Fr-03 Geizhals Guetesiegel Retailer Data Set

Author: FR

Date: June 2024 - ?2024

## Data Set Description / Requirements

**Observational unit**: i, j, t where i ... product (product_id), j ... company (geizhals_firm_id), and t ... week (count var. from UNIX time)

**Period T:**
Potentially the entire period for which we have Geizhals data, but limited from January 1st of the 
previous year to December 31st of the following year of the year in which the quality seal was awarded => [t_ij] <= 52w*3 = 156 weeks
= 3 years as the maximum observation period per **change of quality seal**.

**Companies J**: Potentially all, except Marketplace companies (retailers on the Amazon platform, Shöpping platform).
To reach approximately 18k+ companies, companies with the following terms in their names are excluded from our data-set:
FORBIDDEN_RETAILER_KEYWORDS = ['am-uk', 'am-de', 'am-at', 'eb-uk', 'eb-de', 'sh-at', 'mp-de', 'rk-de', 'nk-pl', 'sz-uk', 'vk-de', 'gx-de']. 
(See project FR-01)
Companies are selected based on the criteria below. (See **Selection criteria**)

**Products I:** Potentially all products, but selected based on the **selection criteria** below.

**Unbalanced sample:** If products or companies are not (or no longer) offered during the (maximally considered) observation period
(3 years, January 1st of the previous year to December 31st of the following year), those observations will be missing.

### Observational unit Selection Criteria

**Step 1**: We use all firms 𝑗 that have experienced a **"change in seal status" S (3 seal dummies)**, meaning they were awarded a seal at a week 𝑡. (see Project FR-01, FH: Requested Data for Seal Awards from the provider "Handelsverhand" so we have specific "Seal award dates" from all 3 Seal providers.) Firms may possess multiple seals (3 seal dummies => 2^3 combinations of bin. vars.) regardless of the date of award.


**Step 2:** For each of these firms 𝑗, we identify the top 200 products i that:

- (1) Attracted the **most clicks** during observation period 𝑡 (3 years => This is currently shortentend to clicks 8 weeks around the seal) within firm 𝑗.
- And were **continuously** offered by firm j **at least two months before and two months after (t-8 weeks, t+8 weeks approx.)** 
the seal award (where continuously = the product was offered at least once every day, Exception one of angebot data week can be missing for product i in the minimum offered time span; *See Test. Angebot data is usually weekly, no Edge-Case was assumed*).


**Step 3:** We identify all other firms (including potentially other seal firms) that 
offered product 𝑖 at the time of seal award. 
From this list of firms, 20 firms are randomly (at maximum) selected. 

If the product is offered by fewer than 20 firms, 
then all firms are selected (Counterfactual firms). (Furhtermore, For base population of "Filtered" (in each year) active geizhals firms see  
Fr-02. (during pre-requirements phase; Data is currently not used in Fr-03)


(Note from FH: If we have fewer than 15 alternative providers for
more than one-third of the products, we need to reconsider the design.)


From these firms (seal firms J_G + counterfactuals J_C), products I_top200 (top 200 from J_G), and time periods ([T_max] ~3 years), the dataset n ≈ count unique obs(i,j,t) is populated. (see Note at end: (Approximative) Resulting maximum dimensions of data set rows based on populated running variables i, j, t)


## Data Set Specifications and Requirements

The final data set specification (which was iterativly refined) 
can be found under Spezifikation_DS_guetesiegel_{VERSION}.doc



## Pre-Checks of (Haendlerstammdatenvariablen) as noted in "requirements.docs"

### Pre-Checks:

#### Check 1 (pre_check_haendler_liefert_vars.py): 
Check whether the information liefert_at / liefert_de (in "haendler.parquet") was already available > 2007 
or whether it was called something different back then.


**Result of Check 1**:

**The current Variables liefert_at and liefert_de look not very useful < 2015. (many NAs -> Imputation)**

Proposed solution: Imputation using the geizhals_bezeichnung or encoding, if imputation not possible.

First and last appearance of Non-Null liefert_at: 1434253021, 1716089821

First and last appearance of Non-Null liefert_de: 1434253021, 1716089821

where 1434253021 approx. 2015

![Pre Check 1 Plot 1](pre_checks/pre_check_1_plot1.png)
![PDF of liefert_at](pre_checks/PDF_of_liefert_at.png)
![PDF of liefert_de](pre_checks/PDF_of_liefert_de.png)


#### Check 2: 
"Check: Whether the information vfb_at > 2007 (in "Verfügbarkeit") was already available or whether it 
was called something else back then.

**Parquet Files, erst ab >2015 da.** -> Imputationsstrategie (siehe Doku)




## Data Set Implementation


### Step 1: Populate Running Variable i,j,t

#### Plausibilty checks of step 1.


At least:
Distinct i's plausible.
Distinct j's plausible.

Number of j's per (i,t) plausible.

Check number of n rows.



### Step 2: Merge populated running varibles i,j,t with existing data sets (e.g. Seal Dummies, Seal Award Date)

#### Check Plausibility of (i,j,t) <-> existing data. (e.g. by computation of a mapped varible by assertion)



### Step 3: Calculate varibles and Instrumental variables based on index space or sub index space (as negotiated in the requirements.docs)

#### Info: Step-Wise Imputation Strategies


## Execution

Execute main.py (for step 1)



### Note: (Approximative) Resulting maximum dimensions of data set rows based on populated running variables i, j, t

To estimate the resulting maximum number of observations, 
I want to find out the maximum combinations of running variables **i**, **j**, **t**.

Let's say we have around ≈ 250 (> 2006) unique firms **F** with seal changes **S**. 
They have together a max average of 1.1 seals **S**. The effective number of seal changes is then SC = 1.1 x 250.

For each seal change firm **SC** at time **t** (week), denote SC_t. 
We scan for the TOP 200 products in the maximum time interval of 3 years around 
the seal change year (3 years ≈ 156 weeks).

SC_t_i is then potentially = 1.1 x 250 x 156 x 200. 
For each product **i**, we look for up to max. 20 counterfactual firms **j**
with the same product **i** that has been continuously offered at
least 8 weeks before and after the seal change.

The resulting maximum **n** is:

n_max =

 1.1 (≈ Max. Average number of seal changes in a seal firm)

x  250 (≈ seal firms > 2006) 

x 156 (maximum observation window in weeks, but a least 16 (8+8) weeks for each selected product) 

x 200 (maximum selected/observerd products per seal firm, can be "TRIMMED" later)

x 20 (maximum counterfactual firm products per seal firm, should hold at least 2 out of 3 times on avg.).

== 171,6 Mio. obs.max.


n_more_realistic =

1.1 (≈ Max. Average number of seal changes in a seal firm)

x 250 (≈ seal firms > 2006) 

x 50 (observation window in weeks, but a least 16 (8+8) weeks for each selected product) 

x 100 (observed products per seal firm)

x 15 (counterfactual firms products per seal firm, should hold at least 2 out of 3 times on avg.).

== 20,625 Mio. obs.max.


Where:
- **j** represents the haendler_bez,
- **i** represents the product ID,
- **t** represents the week running variable.


## Data Set Plausibility Tests

Post population of running variables i,j,t (step 1):
Check if n_min < n_row < n_max and n_rows approximates n_realistic from above.


## Tests

Following tests are currently implemented:

General Tests:
- TestConfig.py
- TestMainFunctionOrder.py

Tests for Methods for implementing the Selection Criteria for calculation of running variables (i, j, t):

- TestFilterContinouslyOfferedProducts.py
- TestGetRandMax20CounterfactualFirms.py
- TestGetStartOfWeek.py
- TestGetTopNProductsbyClicks.py
- TestLoadSealChangeFirmDates.py
- TestIsProductContinouslyOffered.py


