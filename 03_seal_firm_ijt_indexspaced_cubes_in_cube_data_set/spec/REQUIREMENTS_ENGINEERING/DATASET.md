# Requirements Engineering: Data Set Columns Overview

## Description

This specification document outlines the structure and details for the dataset columns used in the requirements engineering process. The following sections cover different types of variables, including those inferred during selection, calculated using selection criteria, and instrumental variables.

### Table Header Description

The following columns describe various aspects of each variable in the dataset.

#### Variable Label

The variable label is meta-information for the dataset to be produced. It should be visible in any statistical program used.

#### Variable Label Description

The variable label description provides additional meta-information for the dataset. It should also be visible in any statistical program used.

#### Variable Description

Further description of the variable, detailing its purpose and context within the dataset.

#### Python Data Type [e.g. str, int, float]

This describes the data type of the realized variable's value, which is usually numeric but may also be a string, especially if it is transformed via a mapper function.

#### Aggregation [e.g. SUM, MIN, MAX]

This specifies how the variable's values have been aggregated. Multiple realizations of the same variable in a domain (table) may require the application of an aggregation function.

#### Calculation [e.g. Formula]

Details the formula used to calculate the variable. This could range from a simple mathematical expression (e.g., a ratio) to a SQL query generated by the business logic abstraction for a given variable specification.

#### Product Variant (i)

Specifies whether the variable's value varies over each product `i`.

#### Firm Variant (j)

Specifies whether the variable's value varies over each firm `j`.

#### Time Variant (t)

Specifies whether the variable's value varies over each time period `t`.

#### Quarterly Variant (q)

Specifies whether the variable's value varies over each quarter `q`.

#### Domain

Typically refers to a folder name, file name, or an existing table name in the system.

#### Business Layer Context

Hints regarding where the implementation of the variable should be located within the business layer.

#### Required Files [.parquet list]

Lists the domains (folders, files, or tables) that will be required for the calculation or storage of the variable.

#### Filter Criteria [if applicable]

Describes any filtering criteria that relate the current entity to an existing table, typically implemented via a `JOIN` operation.

#### Filter Files

Lists the (folders/files or tables) that are required for the filter.

#### Imputation Strategy [Yes/No, Single/Step-Wise?]

Indicates whether imputation will be performed at a later step. If so, specifies whether the imputation is single-step or step-wise.

#### Step-Wise Imputation Preferences

Details the imputation preference relation, e.g., "If firm-level value is not null, use firm-level value. Else, use product-level value. If neither is available, use product-firm-time-level value."

---

### Notations:

- **(h)** ... denotes a helper variable; typically used by another variable or process.

---

## Type 1: Variables / Columns inferred during selection criteria calculation

### Info: Type 1 variables have to be appended during the selection criteria calculation process

| Variable Label    | Variable Label Description                                                         | Variable Description                                   | Python Data Type [e.g. str, int, float] | Aggregation [e.g. SUM, MIN, MAX] | Calculation [e.g. Formula]        | Product variant (i) | Firm variant (j) | Time variant (t) | Quarterly variant (q) | Domain             | Business Layer Context   | Required Files [.parquet list] | Filter Criteria [if applicable] | Filter Files  | Imputation Strategy [Yes/No, Single/Step-Wise?] | Step-Wise Imputation Preferences |
|-------------------|------------------------------------------------------------------------------------|--------------------------------------------------------|----------------------------------------|----------------------------------|----------------------------|---------------------|------------------|------------------|-----------------------|--------------------|--------------------------|--------------------------------|---------------------------------|---------------|-------------------------------------------------|----------------------------------|
| Placeholder       | *Enter description*                                                                | *Enter description*                                    | *Enter data type*                      | *Enter aggregation*              | *Enter calculation*        | *Enter i*           | *Enter j*        | *Enter t*        | *Enter q*             | *Enter domain*     | *Enter context*          | *Enter files*                  | *Enter filter criteria*         | *Enter files* | *Yes/No, Single/Step-Wise?*                     | *Enter preferences*              |
| (h) Top200rank_ij | Rank of product i selected in j's TOP200 list  (Helper Variable for Top200rank_ij) | Rank of product i in firm j’s click list. TOP200 list. | int                                    | -                                | inferred during selection  | YES                 | YES              | NO               | NO                    | Selection Criteria | Selection Criteria Layer | -                              | -                               | -             | -                                               | -                                |

---

## Type 2: Variables / Columns calculated with usage of the selection criteria

### Info: Type 2 variables may be (fully) calculated in groups based on its domain and filter domains instead of rendering batches in the full (i,j,t)-space


| Variable Label | Variable Label Description        | Variable Description                                                                                                                                                                            | Python Data Type [e.g. str, int, float] | Aggregation [e.g. SUM, MIN, MAX] | Calculation [e.g. Formula]                                                                       | Product variant (i) | Firm variant (j) | Time variant (t) | Quarterly variant (q) | Domain                     | Business Layer Context      | Required Files [.parquet list] | Filter Criteria [if applicable] | Filter Files  | Imputation Strategy [Yes/No, Single/Step-Wise?] | Step-Wise Imputation Preferences |
|--------------|-----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|----------------------------------|--------------------------------------------------------------------------------------------------|---------------------|------------------|------------------|-----------------------|----------------------------|-----------------------------|--------------------------------|---------------------------------|---------------|-------------------------------------------------|----------------------------------|
| Placeholder  | *Enter description*               | *Enter description*                                                                                                                                                                            | *Enter data type*                      | *Enter aggregation*              | *Enter calculation*                                                                              | *Enter i*           | *Enter j*        | *Enter t*        | *Enter q*             | *Enter domain*             | *Enter context*             | *Enter files*                  | *Enter filter criteria*         | *Enter files* | *Yes/No, Single/Step-Wise?*                     | *Enter preferences*              |
| Top200rank_i | Rank of product in selection list | Rank of product i in firm j’s click list. Lower rank used if product appears on multiple firms’ TOP200 lists.                                                                                    | int                                    | MIN                              | `SELECT r.product_id, MIN(r.Top200rank_ij) AS Top200rank_i FROM results r GROUP BY r.product_id;` | YES                 | NO               | NO               | NO                   | Selection Criteria results | Results / ImputationService | -                              | -                               | -             | -                                               | Single-Imputation, Product-Level  |

---

## Type 3: Instrumental Variables / Columns calculated after all other columns

### Info: //

| Variable Label  | Variable Label Description        | Variable Description                                                                                                                                                                            | Python Data Type [e.g. str, int, float] | Aggregation [e.g. SUM, MIN, MAX] | Calculation [e.g. Formula]        | Product variant (i) | Firm variant (j) | Time variant (t) | Quarterly variant (q) | Domain         | Business Layer Context | Required Files [.parquet list] | Filter Criteria [if applicable] | Filter Files  | Imputation Strategy [Yes/No, Single/Step-Wise?] | Step-Wise Imputation Preferences |
|-----------------|-----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------|----------------------------------|-----------------------------------|---------------------|------------------|-----------------|-----------------------|----------------|------------------------|--------------------------------|---------------------------------|---------------|-------------------------------------------------|-----------------------------------|
| Placeholder     | *Enter description*               | *Enter description*                                                                                                                                                                            | *Enter data type*                      | *Enter aggregation*              | *Enter calculation*               | *Enter i*           | *Enter j*        | *Enter t*       | *Enter q*             | *Enter domain* | *Enter context*        | *Enter files*                  | *Enter filter criteria*         | *Enter files* | *Yes/No, Single/Step-Wise?*                     | *Enter preferences*              |
| Instrument1_ijt | Instrument for ijt                | Product i belongs to sub-sub-category ssk_quer. Calculates relative price in subsets: Subset 1 without product i, Subset 2 without ssk_quer products.                                            | float                                  | AVG                              | Relative price formula             | YES                 | YES              | YES              | NO                    | -              | -                      | produkt.parquet (?)            | -                               | -             | Yes                                             | Step-Wise                        |