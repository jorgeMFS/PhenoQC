#!/usr/bin/env python3
"""
create_phenoqc_test_data.py

An enhanced script that downloads either:
  - Heart Disease dataset (UCI ID=45), or
  - Chronic Kidney Disease dataset (UCI ID=336)

from the UCI Machine Learning Repository using the 'ucimlrepo' package.

Then it:
  1) Merges the features and target into one CSV,
  2) Injects a new "SampleID" column to satisfy PhenoQC's need for unique IDs,
  3) Creates a JSON schema and a minimal YAML config for PhenoQC.

Outputs are stored in "output/real_data" by default:
   - heart_disease.csv + heart_disease_schema.json + heart_disease_config.yaml
   - OR kidney_disease.csv + kidney_disease_schema.json + kidney_disease_config.yaml

Usage:
  python create_phenoqc_test_data.py [heart|kidney]

Requirements:
  pip install ucimlrepo pyyaml pandas requests

Notes:
  - This script does not attempt any ontology mapping by default, but the YAML config
    references minimal "HPO" and "DO" placeholders. You can remove/modify those as needed.
"""

import sys
import os
import json
import yaml
import pandas as pd

from ucimlrepo import fetch_ucirepo
from requests.exceptions import RequestException

# Destination folder for the output CSV, schema, and config
OUTPUT_DIR = os.path.join("output", "real_data")

def summarize_dataframe(df: pd.DataFrame, dataset_name: str):
    """
    Print a short summary of the DataFrame: row/col count, missing stats, etc.
    """
    row_count, col_count = df.shape
    print(f"[INFO] '{dataset_name}': {row_count} rows x {col_count} columns.")

    # Check if empty
    if df.empty:
        print(f"[WARNING] '{dataset_name}' is empty (0 rows).")
        return

    # Calculate percentage of missing in each column
    missing_perc = df.isna().mean() * 100
    missing_cols = missing_perc[missing_perc > 0].sort_values(ascending=False)
    if not missing_cols.empty:
        print("[INFO] Columns with missing values (descending %):")
        for col, pct in missing_cols.items():
            print(f"   - {col}: {pct:.1f}% missing")
    else:
        print("[INFO] No missing values detected.")

def ensure_output_dir_exists():
    """Makes sure the output directory for real_data exists."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print(f"[INFO] Outputs will be stored in: {OUTPUT_DIR}")

def add_sample_id_column(df: pd.DataFrame, dataset_label: str) -> pd.DataFrame:
    """
    Adds a new 'SampleID' column to the DataFrame if not already present.
    This is critical for PhenoQC (unique IDs).
    We generate it by enumerating the rows: e.g. "heart_0001", "heart_0002", ...
    or "kidney_0001", "kidney_0002", etc.
    """
    if "SampleID" in df.columns:
        print("[WARNING] 'SampleID' column already exists. We'll assume it's valid for unique IDs.")
        return df

    prefix = "heart" if dataset_label.lower() == "heart" else "kidney"
    new_ids = [f"{prefix}_{i+1:05d}" for i in range(len(df))]
    df.insert(0, "SampleID", new_ids)  # put it as the first column
    print(f"[INFO] Added 'SampleID' column with prefix '{prefix}_' to the DataFrame.")
    return df

def create_heart_dataset():
    """
    1) Download Heart Disease dataset (UCI ID=45).
    2) Merge features + target into a single CSV file.
    3) Insert 'SampleID' column.
    4) Create JSON schema + YAML config.
    5) Save them to 'output/real_data'.
    """
    print("[INFO] Attempting to fetch Heart Disease dataset (id=45) from UCI ML Repository...")
    try:
        heart = fetch_ucirepo(id=45)
    except (RequestException, ValueError) as e:
        print(f"[ERROR] Failed to download Heart Disease dataset: {e}")
        sys.exit(1)

    # Combine features & target
    X = heart.data.features
    y = heart.data.targets
    if X is None or y is None or X.empty:
        print("[ERROR] The Heart Disease dataset appears to be empty or invalid.")
        sys.exit(1)

    df = X.copy()
    df["num"] = y  # The original dataset often calls the target "num" or "target"

    # Insert a SampleID column
    df = add_sample_id_column(df, dataset_label="heart")

    # Summarize
    summarize_dataframe(df, "Heart Disease (with SampleID)")

    ensure_output_dir_exists()

    # Write combined CSV
    csv_filename = os.path.join(OUTPUT_DIR, "heart_disease.csv")
    df.to_csv(csv_filename, index=False)
    print(f"[INFO] Created CSV: {csv_filename}")

    # Create JSON schema
    # We add "SampleID" as a required field and mark it as a string.
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Heart Disease Dataset Schema",
        "type": "object",
        "properties": {
            "SampleID":  {"type": "string"},  # newly added ID column
            "age":       {"type": ["number", "null"], "minimum": 0, },
            "sex":       {"type": ["number", "null"],},  # 1=male, 0=female
            "cp":        {"type": ["number", "null"], },
            "trestbps":  {"type": ["number", "null"], "minimum": 0},
            "chol":      {"type": ["number", "null"], "minimum": 0, },
            "fbs":       {"type": ["number", "null"], },
            "restecg":   {"type": ["number", "null"], },
            "thalach":   {"type": ["number", "null"], "minimum": 0, },
            "exang":     {"type": ["number", "null"], },
            "oldpeak":   {"type": ["number", "null"]},  # e.g. can be float
            "slope":     {"type": ["number", "null"], },
            "ca":        {"type": ["number", "null"], },
            "thal":      {"type": ["number", "null"]},  # possibly integer or string in the dataset
            "num":       {"type": ["number", "null"], }  # final target
        },
        "required": ["SampleID", "age", "sex", "cp", "num"]
    }
    schema_filename = os.path.join(OUTPUT_DIR, "heart_disease_schema.json")
    with open(schema_filename, "w") as f:
        json.dump(schema, f, indent=2)
    print(f"[INFO] Created JSON schema: {schema_filename}")

    # YAML config (minimal example with placeholders)
    config = {
        "ontologies": {
            "HPO": {
                "name": "Human Phenotype Ontology",
                "source": "local",
                "file": "path_or_hp.obo",  # Replace with an actual path or use 'url'
                "format": "obo"
            },
            "DO": {
                "name": "Disease Ontology",
                "source": "url",
                "url": "http://purl.obolibrary.org/obo/doid.obo",
                "format": "obo"
            }
        },
        "default_ontologies": ["HPO", "DO"],
        "cache_expiry_days": 30,
        "fuzzy_threshold": 80
    }
    config_filename = os.path.join(OUTPUT_DIR, "heart_disease_config.yaml")
    with open(config_filename, "w") as f:
        yaml.dump(config, f, sort_keys=False)
    print(f"[INFO] Created YAML config: {config_filename}")

def create_kidney_dataset():
    """
    1) Download Chronic Kidney Disease dataset (UCI ID=336).
    2) Merge features + target into a single CSV file.
    3) Insert 'SampleID' column (for unique IDs).
    4) Create a JSON schema + YAML config for PhenoQC.
    5) Store in 'output/real_data' by default.
    """
    print("[INFO] Attempting to fetch Chronic Kidney Disease dataset (id=336) from UCI ML Repository...")
    try:
        kidney = fetch_ucirepo(id=336)
    except (RequestException, ValueError) as e:
        print(f"[ERROR] Failed to download Kidney Disease dataset: {e}")
        sys.exit(1)

    X = kidney.data.features
    y = kidney.data.targets
    if X is None or y is None or X.empty:
        print("[ERROR] The Kidney Disease dataset appears to be empty or invalid.")
        sys.exit(1)

    df = X.copy()
    df["class"] = y  # 'ckd' or 'notckd'

    # Clean up column names (remove spaces/slashes)
    df.columns = [c.strip().replace("/", "_").replace(" ", "_") for c in df.columns]

    # Insert 'SampleID' column if needed
    df = add_sample_id_column(df, dataset_label="kidney")

    summarize_dataframe(df, "Chronic Kidney Disease (with SampleID)")
    ensure_output_dir_exists()

    # Write CSV
    csv_filename = os.path.join(OUTPUT_DIR, "kidney_disease.csv")
    df.to_csv(csv_filename, index=False)
    print(f"[INFO] Created CSV: {csv_filename}")

    # Corrected JSON schema
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Chronic Kidney Disease Dataset Schema",
        "type": "object",
        "properties": {
            "SampleID": {"type": "string"},  # newly added ID column
            "age":      {"type": ["number", "null"], "minimum": 0},
            "bp":       {"type": ["number", "null"], "minimum": 0},
            "sg":       {"type": ["number", "null"]},
            "al":       {"type": ["number", "null"]},
            "su":       {"type": ["number", "null"]},
            "rbc":      {"type": ["string", "null"]},
            "pc":       {"type": ["string", "null"]},
            "pcc":      {"type": ["string", "null"]},
            "ba":       {"type": ["string", "null"]},
            "bgr":      {"type": ["number", "null"], "minimum": 0},
            "bu":       {"type": ["number", "null"], "minimum": 0},
            "sc":       {"type": ["number", "null"], "minimum": 0},
            "sod":      {"type": ["number", "null"]},
            "pot":      {"type": ["number", "null"]},
            "hemo":     {"type": ["number", "null"]},
            "pcv":      {"type": ["number", "null"]},    # in your CSV, it's numeric like 44.0
            "wbcc":     {"type": ["number", "null"]},    # e.g. 7800.0
            "rbcc":     {"type": ["number", "null"]},    # e.g. 5.2
            "htn":      {"type": ["string", "null"]},
            "dm":       {"type": ["string", "null"]},
            "cad":      {"type": ["string", "null"]},
            "appet":    {"type": ["string", "null"]},
            "pe":       {"type": ["string", "null"]},
            "ane":      {"type": ["string", "null"]},
            "class":    {"type": ["string", "null"]}      # 'ckd' or 'notckd'
        },
        # Mark whichever columns must be present. Usually at least these:
        "required": ["SampleID","class"]
        # "required": ["SampleID","pe","ane","class"]
    }
    schema_filename = os.path.join(OUTPUT_DIR, "kidney_disease_schema.json")
    with open(schema_filename, "w") as f:
        json.dump(schema, f, indent=2)
    print(f"[INFO] Created JSON schema: {schema_filename}")

    # Minimal YAML config (no changes needed, but you can edit as desired)
    config = {
        "ontologies": {
            "HPO": {
                "name": "Human Phenotype Ontology",
                "source": "url",
                "url": "http://purl.obolibrary.org/obo/hp.obo",
                "format": "obo"
            },
            "DO": {
                "name": "Disease Ontology",
                "source": "url",
                "url": "http://purl.obolibrary.org/obo/doid.obo",
                "format": "obo"
            }
        },
        "default_ontologies": ["HPO", "DO"],
        "cache_expiry_days": 30,
        "fuzzy_threshold": 80
    }
    config_filename = os.path.join(OUTPUT_DIR, "kidney_disease_config.yaml")
    with open(config_filename, "w") as f:
        yaml.dump(config, f, sort_keys=False)
    print(f"[INFO] Created YAML config: {config_filename}")



def main():
    if len(sys.argv) < 2:
        print("Usage: python create_phenoqc_test_data.py [heart|kidney]")
        sys.exit(1)

    choice = sys.argv[1].lower()
    if choice == "heart":
        create_heart_dataset()
        print("[DONE] Heart Disease data prepared in output/real_data/.")
    elif choice == "kidney":
        create_kidney_dataset()
        print("[DONE] Chronic Kidney Disease data prepared in output/real_data/.")
    else:
        print("ERROR: Unknown argument. Use 'heart' or 'kidney'.")


if __name__ == "__main__":
    main()
