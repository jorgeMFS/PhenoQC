# PhenoQC Synthetic Testing Workflow

This folder hosts a set of Python scripts and configuration files that enable researchers to generate synthetic phenotypic data with realistic anomalies—such as missing values, textual corruption, and duplicates—and then measure how well PhenoQC addresses these issues. By running these scripts either individually or in combination, you can evaluate schema validation, missing-data imputation, duplicate detection, and ontology mapping in a reproducible manner. The following sections detail each script’s role and provide guidance on how to replicate various test scenarios for inclusion in your own experiments or publications.

---

## 1) Scripts Overview

### 1.1 `generate_synthetic_ontology_data.py`

This script creates a CSV file containing numeric and ontology-based text fields. It is the primary generator of synthetic data anomalies like null (missing) cells, duplicate rows, and invalid or corrupted ontology terms. It relies on a `config.yaml` file specifying ontologies (for instance, the Human Phenotype Ontology, Disease Ontology, and Mammalian Phenotype Ontology) which it queries to insert plausible terms into the data. You can control the proportion of missing, duplicate, and corrupted entries by setting numerical rates in the command-line arguments.

Upon execution, this script outputs two files:

1. A `*_data.csv` file containing all generated rows, including any duplicates or invalid text.
2. A `*_ground_truth.csv` file mapping each `SampleID` to its “true” ontology IDs. You only need this file if you plan on measuring how accurately the text fields were mapped to ontology codes in a post-processing step.

**Typical usage** might look like:
```
python generate_synthetic_ontology_data.py \
  --config ./config/config.yaml \
  --num_records 3000 \
  --missing_rate 0.2 \
  --duplicate_rate 0.1 \
  --invalid_rate 0.05 \
  --output_prefix ./mydata/test_scenario
```
This prints a final report indicating how many rows were created, how many were duplicated, and how many contained corrupted text fields.

---

### 1.2 `phenoqc_benchmark.py`

After you generate a CSV with synthetic data, `phenoqc_benchmark.py` is used to run the PhenoQC toolkit on that file and then parse the outcomes of the run. Internally, this script:

1. Invokes `phenoqc` in a subprocess.
2. Locates the processed CSV that `phenoqc` produces (noting that PhenoQC often adds hashes or suffixes to the output filename).
3. Loads the newly produced CSV and measures:
   - The amount of missing data that remains post-imputation for specified columns.
   - The number of duplicates flagged, given a list of unique identifier columns (e.g., `SampleID`).
   - Any schema violations that occurred if the data did not conform to the JSON schema defined in `schema.json`.

You can specify which columns to check for missingness via `--check_columns`, and you can select an imputation strategy (mean, median, mode, etc.) for numeric fields via `--impute_strategy`. An example:

```
python phenoqc_benchmark.py \
  --input_data ./mydata/test_scenario_data.csv \
  --config ./config/config.yaml \
  --schema ./config/schema.json \
  --output_dir ./myreports \
  --uniqueIDs SampleID \
  --phenotype_column PrimaryPhenotype \
  --impute_strategy mean \
  --check_columns Height_cm Weight_kg Cholesterol_mgdl
```

In a typical console output, you will see how many rows had duplicates, how many columns still contained missing values after imputation, and whether any records triggered schema validation errors.

---

### 1.3 `check_ontology_accuracy.py`

If you want to evaluate how effectively PhenoQC (or any text-matching module) mapped your textual fields onto ontology IDs, this script provides a standalone accuracy check. It compares columns in the final PhenoQC-processed CSV—such as `HPO_ID`, `DO_ID`, `MPO_ID`—against the “true” IDs listed in the `*_ground_truth.csv` file from your synthetic data generation step.  

You run it like this:
```
python check_ontology_accuracy.py \
  --processed_csv ./myreports/test_scenario_data_abc123_csv.csv \
  --ground_truth  ./mydata/test_scenario_ground_truth.csv \
  --id_column SampleID \
  --hpo_column HPO_ID \
  --do_column DO_ID \
  --mpo_column MPO_ID
```
It merges both files on `SampleID` and prints lines indicating the percentage of records whose predicted ID matches the ground-truth ID exactly (e.g., “Accuracy for HPO_ID vs. TruePrimaryHPO_ID: 92.00% on 2800 records.”). You can omit this step if you do not need to verify ontology mappings.

---

### 1.4 `unified_scenarios_test.py`

When you want to repeat a series of tests covering multiple parameter values (e.g., different dataset sizes or missing-value rates), you can use `unified_scenarios_test.py` to automate the process. This script orchestrates:

1. Calling `generate_synthetic_ontology_data.py` for each scenario parameter (e.g., `num_records = 10k, 50k, 100k`).
2. Immediately running `phenoqc_benchmark.py` on the generated data.
3. Storing relevant metrics—like final row counts, duplicates found, schema violations, and post-imputation missingness—in a single summary CSV.

By default, `unified_scenarios_test.py` focuses on numeric and structural checks, so it does **not** invoke `check_ontology_accuracy.py` automatically. If your primary goal is to evaluate missing data or to scale up to larger datasets, this script alone will suffice. If you also want ontology accuracy results, you would run `check_ontology_accuracy.py` separately on the final CSV of whichever scenario run you are interested in.

A simple invocation is:
```
python unified_scenarios_test.py
```
It will execute all scenario definitions listed at the top of the file (SCALING, MISSINGNESS, CORRUPTION, DUPLICATE_STRESS) and create outputs in a directory (e.g., `./unified_scenarios_outputs/`). Within that folder, the script writes a CSV summarizing each run’s performance metrics.

---

## 2) Typical Workflow for Testing Missing Data and Ontology Mapping

If you want to demonstrate in a paper how PhenoQC manages missing values and textual mappings, you might follow these steps:

1. **Set Up Configuration and Schema**  
   In `config.yaml`, define the relevant ontologies (HPO, DO, MPO, etc.). In `schema.json`, specify any required fields or value constraints. Confirm that the columns you want to evaluate for missingness (e.g., `Height_cm`, `Weight_kg`, `Cholesterol_mgdl`) match your schema definition.

2. **Generate a Synthetic Dataset**  
   Use `generate_synthetic_ontology_data.py` to create a CSV that intentionally has missing numeric fields (like 10% missing rate) and a portion of corrupted text (say 5% invalid terms in the phenotype columns). For instance:

   ```
   python generate_synthetic_ontology_data.py \
     --config ./config/config.yaml \
     --num_records 3000 \
     --missing_rate 0.1 \
     --duplicate_rate 0.05 \
     --invalid_rate 0.05 \
     --output_prefix ./test_scenario
   ```

   This will create `test_scenario_data.csv` and `test_scenario_ground_truth.csv`.

3. **Benchmark PhenoQC**  
   Run `phenoqc_benchmark.py` on that newly generated file to see:
   - How many duplicates remain (and whether they match your expected fraction).
   - How many missing cells are present after the chosen imputation strategy (mean, median, etc.).
   - Whether any rows violate the schema:

   ```
   python phenoqc_benchmark.py \
     --input_data ./test_scenario_data.csv \
     --config ./config/config.yaml \
     --schema ./config/schema.json \
     --output_dir ./results \
     --uniqueIDs SampleID \
     --impute_strategy mean \
     --phenotype_column PrimaryPhenotype \
     --check_columns Height_cm Weight_kg Cholesterol_mgdl
   ```

   Check the console output for lines indicating `[WARNING] Found X duplicates` or `missing=X (Y%)`.

4. **Examine Ontology Mapping (Optional)**  
   If you would like to confirm that textual descriptors such as “Hypertension ???(typo)” were correctly mapped to an HPO ID, run `check_ontology_accuracy.py`:

   ```
   python check_ontology_accuracy.py \
     --processed_csv ./results/test_scenario_data_???_csv.csv \
     --ground_truth  ./test_scenario_ground_truth.csv \
     --hpo_column HPO_ID \
     --do_column DO_ID \
     --mpo_column MPO_ID
   ```
   This script prints out the percentage of rows where, for example, `HPO_ID` equals the “true” HPO ID from ground truth. If you have a second HPO column (like `SecondaryPhenotype`), you can add `--secondary_hpo_column`.

5. **Scale Up or Automate**  
   Finally, if you want to replicate multiple runs for different parameter values (e.g., 5% vs. 20% vs. 50% missingness), you can do so manually or use `unified_scenarios_test.py`. Running `python unified_scenarios_test.py` loops over each scenario defined in the file and writes a summary to `unified_scenarios_summary.csv`. You can still run `check_ontology_accuracy.py` on any final CSV that interests you, though that step must be done separately.

---

## 3) Conclusion

These scripts, in combination, offer a flexible and reproducible pipeline for evaluating PhenoQC under conditions that mimic real-world phenotypic data challenges. You can focus solely on numeric validations or expand your analysis to ontology mapping accuracy, all while controlling how much corruption, missingness, or duplication is injected. Each individual script targets a specific portion of this workflow:

1. `generate_synthetic_ontology_data.py` for data creation.  
2. `phenoqc_benchmark.py` for validating and measuring duplicates and missing data after PhenoQC’s processes.  
3. `check_ontology_accuracy.py` for assessing how faithfully textual fields match ground-truth ontology IDs.  
4. `unified_scenarios_test.py` for automating scenario-driven testing across multiple parameters.

By mixing and matching these scripts—or by using them all in a staged approach—you can produce detailed results on how well PhenoQC handles schema validation, missing-data imputation, duplicate detection, and ontology term mapping in synthetic, controlled experiments.