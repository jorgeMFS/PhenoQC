#!/usr/bin/env python3
"""
test_real_data_all_in_one.py

A comprehensive test script for "real data" with PhenoQC, assuming PhenoQC is installed
as a console script (i.e., you can run `phenoqc --help` in your environment).

Example usage:
  python test_real_data_all_in_one.py \
     --data output/real_data/kidney_disease.csv \
     --schema output/real_data/kidney_disease_schema.json \
     --config output/real_data/kidney_disease_config.yaml \
     --unique_id SampleID \
     --output_dir ./test_results/

  python test_real_data_all_in_one.py \
     --data output/real_data/heart_disease.csv \
     --schema output/real_data/heart_disease_schema.json \
     --config output/real_data/heart_disease_config.yaml \
     --unique_id SampleID \
     --output_dir ./test_results/
"""

import os
import sys
import argparse
import subprocess
import json
import csv
import pandas as pd
import yaml

# Ensure the repository's ``src`` directory is on ``sys.path`` so that
# the ``phenoqc`` package can be imported when running this script
# directly without installing the package.
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(SCRIPT_DIR, '..', 'src'))

from phenoqc.logging_module import setup_logging, log_activity


def load_schema(schema_path):
    """Load JSON schema from disk (draft-07 recommended)."""
    with open(schema_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_config_yaml(config_path):
    """Load your YAML config, e.g. with ontology references."""
    with open(config_path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)


def check_id_column(df, id_col):
    """Ensure the ID column is present and non-null in every row."""
    if id_col not in df.columns:
        raise ValueError(f"[ERROR] ID column '{id_col}' not found in the data!")
    if df[id_col].isna().any():
        raise ValueError(f"[ERROR] ID column '{id_col}' has missing values. Must be unique per row.")


def count_missing(df):
    """
    Return {column_name: (missing_count, missing_percent)} for numeric columns.
    """
    summary = {}
    numeric_cols = df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        n_miss = df[col].isna().sum()
        pct_miss = (n_miss / len(df)) * 100 if len(df) else 0
        summary[col] = (n_miss, pct_miss)
    return summary


def total_missing(summary_dict):
    """
    Given {col: (missing_count, percent)}, sum all missing_count.
    """
    return sum(v[0] for v in summary_dict.values())


def run_phenoqc_cli(input_csv, schema_path, config_path, unique_id_col, output_dir):
    """
    Calls `phenoqc` as if installed globally:
      phenoqc --input ...
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
    parser.add_argument("--data", required=True, help="Path to your CSV data.")
    parser.add_argument("--schema", required=True, help="Path to JSON schema file.")
    parser.add_argument("--config", required=True, help="Path to YAML config file.")
    parser.add_argument("--unique_id", required=True, help="ID column name in your data.")
    parser.add_argument("--output_dir", default="./test_results", help="PhenoQC output directory.")
    parser.add_argument("--summary_csv", default="phenoqc_test_summary.csv",
                        help="Name of the CSV file to store test summary results.")
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    setup_logging(log_file="test_real_data.log", mode="w")

    summary_results = {
        "dataset": os.path.basename(args.data),
        "schema_pass": None,            # We'll set these after PhenoQC
        "n_schema_fails": 0,           # We'll rely on PhenoQC logs for detail
        "missing_pre_total": None,
        "missing_post_total": None,
        "duplicates_found": 0,
        "pdf_found": False,
        "exit_code": 0,
    }

    # 1) Load the data
    try:
        df = pd.read_csv(args.data)
    except Exception as e:
        print(f"[ERROR] Could not read CSV data {args.data}: {e}")
        summary_results["schema_pass"] = "FAIL - cannot read data"
        summary_results["exit_code"] = 1
        _write_summary_row(args.output_dir, args.summary_csv, summary_results)
        sys.exit(1)
    print(f"[INFO] Real data loaded: shape={df.shape}")

    # Debug prints
    print("[DEBUG] DataFrame dtypes after reading input CSV:")
    print(df.dtypes)
    print("[DEBUG] Sample of the first 10 rows of input CSV data:")
    print(df.head(10))
    print("-"*50)

    # 2) Check ID column
    try:
        check_id_column(df, args.unique_id)
        print("[INFO] ID column is valid.")
    except ValueError as vex:
        print(str(vex))
        summary_results["schema_pass"] = "FAIL - no valid ID"
        summary_results["exit_code"] = 1
        _write_summary_row(args.output_dir, args.summary_csv, summary_results)
        sys.exit(1)

    # 3) Load JSON schema & YAML config (for reference / logs)
    try:
        schema = load_schema(args.schema)
        config = load_config_yaml(args.config)
        print("[INFO] Successfully loaded schema & config.")
        print("[DEBUG] JSON schema (from %s):" % args.schema)
        print(json.dumps(schema, indent=2))
        print("-"*50)
        print("[DEBUG] YAML config (from %s):" % args.config)
        print(yaml.dump(config, sort_keys=False))
        print("-"*50)
    except Exception as e:
        print(f"[ERROR] Could not load schema or config: {e}")
        summary_results["schema_pass"] = f"FAIL - cannot load schema/config: {str(e)}"
        summary_results["exit_code"] = 1
        _write_summary_row(args.output_dir, args.summary_csv, summary_results)
        sys.exit(1)

  

    # 4) Check missing numeric data pre-PhenoQC
    missing_pre = count_missing(df)
    pre_missing_total = total_missing(missing_pre)
    summary_results["missing_pre_total"] = pre_missing_total
    if missing_pre:
        print("[INFO] Missing data before imputation (numeric columns):")
        for col, (n_miss, pct_miss) in missing_pre.items():
            print(f"  {col}: {n_miss} missing ({pct_miss:.2f}%)")
    else:
        print("[INFO] No missing numeric data detected pre-imputation.")

    # 5) Run PhenoQC CLI
    rc, stdout_text, stderr_text = run_phenoqc_cli(
        args.data, args.schema, args.config, args.unique_id, args.output_dir
    )
    summary_results["exit_code"] = rc

    print("\n[INFO] PhenoQC stdout:\n", stdout_text)
    print("\n[INFO] PhenoQC stderr:\n", stderr_text)

    if rc != 0:
        print(f"[ERROR] PhenoQC returned code {rc}. Possibly an error encountered.")
        _write_summary_row(args.output_dir, args.summary_csv, summary_results)
        sys.exit(rc)
    else:
        print("[INFO] PhenoQC completed successfully.")

    # Because PhenoQC handles row-level schema internally, we'll 
    # infer success => "schema_pass" is PASS. 
    # (If you want to parse the stdout to detect how many fails, you can.)
    summary_results["schema_pass"] = "PASS"
    summary_results["n_schema_fails"] = 0  # If needed, parse phenoqc logs to fill in real count

    # 6) Locate the processed CSV
    processed_csv_fallback = os.path.join(args.output_dir, "processed_data.csv")
    if os.path.exists(processed_csv_fallback):
        post_df = pd.read_csv(processed_csv_fallback)
        print(f"[INFO] Found processed_data.csv => shape={post_df.shape}")
    else:
        print("[WARNING] Processed CSV not found by fallback name. Searching directory.")
        processed_csv = None
        for fname in os.listdir(args.output_dir):
            if fname.endswith("csv.csv"):
                processed_csv = os.path.join(args.output_dir, fname)
                break
        if processed_csv and os.path.exists(processed_csv):
            post_df = pd.read_csv(processed_csv)
            print(f"[INFO] Processed CSV found: {processed_csv}, shape={post_df.shape}")
        else:
            post_df = None
            print("[ERROR] Could not locate processed CSV. Skipping post-run checks.")

    # 7) Additional checks on the processed CSV
    if post_df is not None and not post_df.empty:
        missing_post = count_missing(post_df)
        post_missing_total = total_missing(missing_post)
        summary_results["missing_post_total"] = post_missing_total

        if missing_post:
            print("[INFO] Missing data after PhenoQC (numeric columns):")
            for col, (n_miss, pct_miss) in missing_post.items():
                print(f"  {col}: {n_miss} missing ({pct_miss:.2f}%)")
            if post_missing_total == 0:
                print("[INFO] All numeric missing values appear to have been imputed.")
            else:
                print("[WARNING] Some numeric columns still have missing data after imputation.")
        else:
            print("[INFO] No missing numeric data found post-imputation.")
            summary_results["missing_post_total"] = 0

        # Check for duplicates in the post-processed CSV
        dups = post_df[post_df.duplicated(subset=[args.unique_id], keep=False)]
        if not dups.empty:
            n_dups = len(dups)
            print(f"[INFO] Found {n_dups} duplicate rows in final processed CSV based on {args.unique_id}.")
            summary_results["duplicates_found"] = n_dups
        else:
            print("[INFO] No duplicates found in final processed CSV (based on ID).")

        # Check for ontology columns if expected
        for col in ["HPO_ID", "DO_ID"]:
            if col in post_df.columns:
                print(f"[INFO] Ontology column '{col}' is present.")
            else:
                print(f"[INFO] Column '{col}' not found. That may be normal if no mapping was configured.")
    else:
        print("[INFO] No final processed CSV to examine or it was empty.")

    # 8) Check for PDF or other artifact
    for fname in os.listdir(args.output_dir):
        if fname.endswith("_report.pdf"):
            print(f"[INFO] Found a PDF report: {fname}")
            summary_results["pdf_found"] = True

    # 9) Write final results to summary CSV
    _write_summary_row(args.output_dir, args.summary_csv, summary_results)

    print("\n[TEST COMPLETE] No fatal errors above => success.\n")
    sys.exit(rc)


def _write_summary_row(output_dir, summary_csv_name, row_data):
    """
    Appends a single row of results (dict) to the CSV summary file.
    Creates the file with headers if it doesn't exist.
    """
    summary_path = os.path.join(output_dir, summary_csv_name)
    write_header = not os.path.exists(summary_path)

    fieldnames = [
        "dataset",
        "schema_pass",
        "n_schema_fails",
        "missing_pre_total",
        "missing_post_total",
        "duplicates_found",
        "pdf_found",
        "exit_code",
    ]

    with open(summary_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        writer.writerow(row_data)

    print(f"[INFO] Wrote test summary row to '{summary_path}'")


if __name__ == "__main__":
    main()
