# Fr-01 Guetesiegel Geizhals Retailer (Matching)

Author: FR

Date: April 2024 - May 2024

## Execution

Run the script to perform these steps and review the results in the console and output files.

## Description of the Script

### Step 0: Geizhals Retailer Filtering

**Input:** `input/haendler.parquet`  
**Output:** `output/filtered_haendler_bez.csv`

This step involves filtering out retailers based on specific keywords (e.g., 'am-uk', 'am-de') from the input dataset `haendler.parquet`. After execution, the resulting `filtered_haendler_bez.csv` file should contain approximately 18k+ retailers out of the 2 million+ geizhals retailers.

### Step 1: Guetesiegel Geizhals Retailer Matching (on geizhals_bez)

In this step, the filtered data is matched against external datasets obtained from the "Guetesiegel"-Provider. These datasets include:
- `e_commerce.csv`
- `ehi_bevh.csv`
- `handelsverband.csv`

The matching process includes several steps:
- **Simple Matching:** Initial matching.
- **Pre-filling Matches:** Prefills the column "RESULTING MATCH" with data from Simple Matching.
- **JARO Matching:** Uses the Jaro similarity index to find the top-1 and 3-closest matches.
- **Advanced Matching:** Further refines the matching process using start and end substring-matching techniques.

The matched results are stored in the following output files:
- `e-commerce_matched.csv`
- `ehi_bevh_matched.csv`
- `handelsverband_matched.csv`

### Step 2: Post-Matching Processing (Review of Match Candidates)

In this step, manual review is conducted for retailers with lower probability matches. These retailers are reviewed and matched manually, and the results are stored in columns labeled "RESULTING MATCH" in the following datasets:
- `e_commerce_reviewed.csv`
- `ehi_bevh_reviewed.csv`
- `handelsverband_reviewed.csv`

### Step 3: Merge the Reviewed Matches with the Geizhals Retailer

This step involves merging the reviewed matches with the geizhals retailer dataset to include some geizhals dummy variables such as 'is_at' and 'is_de'. The process includes:
1. **Input:** 
   - `e_commerce_reviewed.csv`
   - `ehi_bevh_reviewed.csv`
   - `handelsverband_reviewed.csv`
2. **Output:** (left merge on `RESULTING_MATCH = geizhals_bez`)
   - `final_matrix`
3. **Additional Output (Code Commented for Future Use if Necessary):**
   - `e_commerce_reviewed.csv_merged`
   - `ehi_bevh_reviewed.csv_merged`
   - `handelsverband_reviewed.csv_merged`

### Step 4: Produce Some Descriptive Statistics

This step produces Guetesiegel count statistics of matched, reviewed, and merged geizhals retailers in the file `output/count_matrix.csv`.
The resulting Pivot tabular will look like this:

| filenamecp                  | is_at | is_de | 2000 | 2001 | 2002 | 2004 | 2005 | 2006 | 2007 | 2008 | 2009 | 2010 | 2011 | 2012 | 2013 | 2014 | 2015 | 2016 | 2017 | 2018 | 2019 | 2020 | 2021 | 2022 | SUMME |
| --------------------------- | ----- | ----- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ---- | ----- |
| e_commerce_reviewed.csv     | 0     | 1     | 0    | 0    | 1    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 1    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    |       |
| e_commerce_reviewed.csv     | 1     | 0     | 0    | 1    | 0    | 1    | 3    | 1    | 1    | 4    | 2    | 0    | 6    | 1    | 0    | 2    | 2    | 3    | 3    | 3    | 0    | 3    | 2    | 1    |       |
| ehi_bevh_reviewed.csv       | 0     | 1     | 0    | 3    | 1    | 2    | 6    | 2    | 1    | 8    | 7    | 7    | 10   | 4    | 8    | 4    | 7    | 9    | 5    | 2    | 5    | 2    | 2    | 5    |       |
| ehi_bevh_reviewed.csv       | 1     | 0     | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 1    | 2    | 0    |       |
| handelsverband_reviewed.csv | 1     | 0     | 5    | 0    | 0    | 0    | 0    | 0    | 0    | 0    | 1    | 0    | 2    | 1    | 1    | 1    | 1    | 2    | 0    | 0    | 1    | 2    | 2    | 0    |       |
|                             |       |       |      |      |      |      |      |      |      |      |      |      |      |      |      |      |      |      |      |      |      |      |      |      | 163   |

### Step 5. Review the results in the output files located in the `output/` folder.

## CONFIG.py

The script utilizes a configuration file named `CONFIG.py` to set global constants and parameters. Here are some key constants used in the script:

- **ALLOW_SKIPPING:** A boolean constant determining whether certain steps in the script can be skipped if the output files already exist.
- **CSV_SEPARATOR:** Defines the separator used in CSV files.
- **INPUT_FOLDER:** Path to the folder containing input files.
- **OUTPUT_FOLDER:** Path to the folder where output files will be stored.
- Other constants define input and output file paths and step-specific parameters.

## Testing

The script includes a unit test module located in the `test` folder.
The tests ensure the functionality of the `MatchingCriteria` class
, which includes methods for advanced matching criteria used in the script.
To run the tests, navigate to the `test` folder in your
terminal and execute the following command: python -m unittest
