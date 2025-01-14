#!/usr/bin/env python3
"""
test_real_data_all_in_one.py

A comprehensive test script for "real data" with PhenoQC, assuming PhenoQC is installed
as a console script (i.e., you can run `phenoqc --help` in your environment).

python test_real_data_all_in_one.py \
     --data output/real_data/heart_disease.csv \
     --schema output/real_data/heart_disease_schema.json \
     --config output/real_data/heart_disease_config.yaml \
     --unique_id SampleID \
     --output_dir ./test_results/

     
python test_real_data_all_in_one.py \
     --data output/real_data/kidney_disease.csv \
     --schema output/real_data/kidney_disease_schema.json \
     --config output/real_data/kidney_disease_config.yaml \
     --unique_id SampleID \
     --output_dir ./test_results/


"""

import os
import sys
import argparse
import subprocess
import json
import pandas as pd
import yaml
from validation import DataValidator
from logging_module import setup_logging, log_activity


def load_schema(schema_path):
    with open(schema_path, 'r') as f:
        return json.load(f)

def load_config_yaml(config_path):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def check_id_column(df, id_col):
    if id_col not in df.columns:
        raise ValueError(f"[ERROR] ID column '{id_col}' not found in the data!")
    if df[id_col].isna().any():
        raise ValueError(f"[ERROR] ID column '{id_col}' has missing values. Must be unique per row.")

def run_schema_validation(df, schema, id_col):
    validator = DataValidator(df, schema, unique_identifiers=[id_col])
    results = validator.run_all_validations()
    if not results["Format Validation"]:
        failing = results["Integrity Issues"]
        return False, failing
    else:
        return True, pd.DataFrame()

def count_missing(df):
    """
    Return a dictionary: {column_name: (missing_count, missing_percent)}
    for numeric columns. Adapt if you want all columns.
    """
    summary = {}
    numeric_cols = df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        miss = df[col].isna().sum()
        pct = (miss / len(df)) * 100 if len(df) else 0
        summary[col] = (miss, pct)
    return summary

def run_phenoqc_cli(input_csv, schema_path, config_path, unique_id_col, output_dir):
    """
    Calls `phenoqc` from your PATH (assuming `phenoqc` is installed).
    If that command is not on PATH, you'll see a 'No such file' error.
    """
    cmd = [
        "phenoqc",
        "--input", input_csv,
        "--schema", schema_path,
        "--config", config_path,
        "--unique_identifiers", unique_id_col,
        "--output", output_dir
    ]
    print(f"[INFO] Running PhenoQC with command:\n   {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr

def main():
    parser = argparse.ArgumentParser(description="Comprehensive real data test for PhenoQC (installed).")
    parser.add_argument("--data", required=True, help="Path to the real CSV data.")
    parser.add_argument("--schema", required=True, help="Path to the JSON schema file.")
    parser.add_argument("--config", required=True, help="Path to the YAML config file.")
    parser.add_argument("--unique_id", required=True, help="Name of the ID column in your data.")
    parser.add_argument("--output_dir", default="./test_results", help="Where to store PhenoQC output.")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    setup_logging(log_file="test_real_data.log", mode="w")

    # 1) Load data
    try:
        df = pd.read_csv(args.data)
    except Exception as e:
        print(f"[ERROR] Could not read CSV data {args.data}: {e}")
        sys.exit(1)

    print(f"[INFO] Real data loaded: shape={df.shape}")

    # 2) Check ID column presence & non-null
    try:
        check_id_column(df, args.unique_id)
        print("[INFO] ID column is valid.")
    except ValueError as vex:
        print(str(vex))
        sys.exit(1)

    # 3) Load schema & config
    try:
        schema = load_schema(args.schema)
        config = load_config_yaml(args.config)
        print("[INFO] Successfully loaded schema & config.")
    except Exception as e:
        print(f"[ERROR] Could not load schema or config: {e}")
        sys.exit(1)

    # 4) Quick row-level schema validation (optional, can be commented out)
    pass_schema, failing_df = run_schema_validation(df, schema, args.unique_id)
    if not pass_schema:
        print("[ERROR] Some rows fail the JSON schema. Example failing rows:")
        print(failing_df.head(5))
        failing_csv = os.path.join(args.output_dir, "schema_failures.csv")
        failing_df.to_csv(failing_csv, index=False)
        print(f"[INFO] Wrote failing rows to {failing_csv}")
    else:
        print("[INFO] All rows pass JSON schema at row-level.")

    # 5) Missing data check (pre-imputation)
    missing_pre = count_missing(df)
    if missing_pre:
        print("[INFO] Missing data before imputation (numeric columns):")
        for col, (n_miss, pct_miss) in missing_pre.items():
            print(f"  {col}: {n_miss} missing ({pct_miss:.2f}%)")
    else:
        print("[INFO] No missing numeric data detected pre-imputation.")

    # 6) Run PhenoQC CLI (installed)
    rc, stdout_text, stderr_text = run_phenoqc_cli(args.data, args.schema, args.config, args.unique_id, args.output_dir)
    print("[INFO] PhenoQC stdout:\n", stdout_text)
    print("[INFO] PhenoQC stderr:\n", stderr_text)
    if rc != 0:
        print(f"[ERROR] PhenoQC returned code {rc}. Possibly an error encountered.")
        sys.exit(rc)
    else:
        print("[INFO] PhenoQC completed successfully.")

    # 7) Optional: parse or locate processed CSV, check missingness again, etc.

    print("\n[TEST COMPLETE] No fatal errors above => success.\n")

if __name__ == "__main__":
    main()
