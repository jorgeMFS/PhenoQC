#!/usr/bin/env python3

"""
phenoqc_benchmark.py

Example usage (for your synthetic_phenotypic_data.csv):
  python phenoqc_benchmark.py \
    --input_data ../study/synthetic_phenotypic_data.csv \
    --config config.yaml \
    --schema schema.json \
    --output_dir ./reports \
    --uniqueIDs SampleID \
    --impute_strategy mean \
    --phenotype_column PrimaryPhenotype

Explanation:
  - We pass one or more --uniqueIDs to indicate which columns uniquely identify each record.
  - For the synthetic dataset, "SampleID" typically is enough.
  - The script calls "phenoqc" in a subprocess, times it, and then loads the processed CSV 
    to perform a few basic checks (missing data stats, duplicate check, etc.).
"""

import subprocess
import time
import argparse
import os
import pandas as pd

def run_phenoqc(args):
    """
    Invokes the phenoqc CLI in a subprocess; returns (exit_code, runtime_seconds).
    """
    cli_cmd = [
        "phenoqc",
        "--input", args.input_data,
        "--output", args.output_dir,
        "--schema", args.schema,
        "--config", args.config,
        "--unique_identifiers"
    ]
    # Add each unique ID
    cli_cmd.extend(args.uniqueIDs)
    
    # Optional
    cli_cmd += ["--impute", args.impute_strategy]
    cli_cmd += ["--phenotype_column", args.phenotype_column]

    if args.ontologies:
        cli_cmd.append("--ontologies")
        cli_cmd.extend(args.ontologies)

    if args.recursive:
        cli_cmd.append("--recursive")

    print("[INFO] Running command:\n  ", " ".join(cli_cmd))
    t0 = time.time()
    completed = subprocess.run(cli_cmd, capture_output=True, text=True)
    elapsed = time.time() - t0
    
    print("\n[STDOUT]:\n", completed.stdout)
    print("[STDERR]:\n", completed.stderr)
    
    return completed.returncode, elapsed

def analyze_missing_data(df, columns=None):
    """
    Show basic missing-data stats for specified columns (or all if None).
    Returns a dict for each column: {missing_count, missing_percent}.
    """
    if columns is None:
        columns = df.columns
    stats = {}
    for col in columns:
        n_missing = df[col].isna().sum()
        pct = 100.0 * n_missing / len(df) if len(df) else 0
        stats[col] = (n_missing, pct)
    return stats

def main():
    parser = argparse.ArgumentParser(description="Benchmark & Validation for PhenoQC.")
    parser.add_argument("--input_data", required=True, help="Path to CSV/TSV/JSON input file.")
    parser.add_argument("--config", default="config.yaml", help="Path to config.yaml.")
    parser.add_argument("--schema", default="schema.json", help="Path to JSON schema.")
    parser.add_argument("--output_dir", default="./reports", help="Where to store reports and processed CSV.")
    parser.add_argument("--uniqueIDs", nargs="+", required=True, help="Unique identifier column(s). E.g. SampleID")
    parser.add_argument("--ontologies", nargs="+", default=None, help="Ontologies to map (e.g., HPO DO etc.).")
    parser.add_argument("--phenotype_column", default="Phenotype", help="Phenotype column name in your data.")
    parser.add_argument("--impute_strategy", default="mean", help="Imputation strategy (mean, median, mode, etc.).")
    parser.add_argument("--recursive", action="store_true", help="If input_data is a directory and you want recursion.")
    parser.add_argument("--check_columns", nargs="+", default=None, help="Columns to check for missing-data stats.")
    args = parser.parse_args()

    # 1) Run the pipeline
    ret_code, elapsed = run_phenoqc(args)
    print(f"[INFO] phenoqc exit code={ret_code}, time={elapsed:.2f}s")

    # 2) Build the expected processed filename
    basename = os.path.basename(args.input_data)
    processed_file_path = os.path.join(args.output_dir, basename)
    if not os.path.isfile(processed_file_path):
        print(f"[ERROR] Processed file not found: {processed_file_path}")
        return

    # 3) Load processed data
    df = pd.read_csv(processed_file_path)
    print(f"[INFO] Processed CSV shape: {df.shape}")

    # 4) Missing data analysis
    missing_stats = analyze_missing_data(df, args.check_columns)
    print("[INFO] Missing data (post-imputation) in selected columns:")
    for col, (count, pct) in missing_stats.items():
        print(f"  {col}: missing={count} ({pct:.2f}%)")

    # 5) Duplicate check (just a final quick check)
    if len(args.uniqueIDs) > 0:
        dups = df[df.duplicated(subset=args.uniqueIDs, keep=False)]
        if not dups.empty:
            print(f"[WARNING] Found {len(dups)} duplicates in final CSV based on {args.uniqueIDs}!")
        else:
            print("[INFO] No duplicates found in final CSV based on uniqueIDs.")

    print("\n[DONE] Benchmark script completed.")

if __name__ == "__main__":
    main()


# python phenoqc_benchmark.py   --input_data ../study/synthetic_phenotypic_data.csv   --config ./config/config.yaml   --schema ./config/schema.json   --output_dir ../reports   --uniqueIDs SampleID   --impute_strategy mean   --phenotype_column PrimaryPhenotype
