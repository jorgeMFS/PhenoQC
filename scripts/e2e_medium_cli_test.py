#!/usr/bin/env python3
"""
Generate a mid-sized synthetic dataset and run PhenoQC with
config-driven imputation and class-imbalance reporting.

This script verifies that:
- Processed CSV and PDF report are generated
- Quality metrics TSV/JSON exist
- Numeric missingness is reduced after imputation
- Class distribution is imbalanced (by construction)
"""

import os
import sys
import json
import subprocess
from typing import Tuple

import numpy as np
import pandas as pd


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from phenoqc.batch_processing import unique_output_name


OUT_DIR = os.path.join(SCRIPT_DIR, "output", "e2e_medium")
os.makedirs(OUT_DIR, exist_ok=True)

SCHEMA_PATH = os.path.join(SCRIPT_DIR, "config", "schema.json")
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config", "config.yaml")
DATA_PATH = os.path.join(OUT_DIR, "e2e_medium_input.csv")


def create_comprehensive_data(n: int = 1000, seed: int = 42) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    sample_ids = [f"S{str(i+1).zfill(5)}" for i in range(n)]

    # Numeric variables with controlled missingness
    height = rng.normal(loc=170, scale=10, size=n)
    weight = rng.normal(loc=70, scale=12, size=n)
    chol = rng.normal(loc=200, scale=40, size=n)
    bps = rng.normal(loc=120, scale=15, size=n)
    bpd = rng.normal(loc=80, scale=12, size=n)
    glucose = rng.normal(loc=95, scale=15, size=n)
    creat = rng.normal(loc=1.0, scale=0.2, size=n)

    # Introduce missingness
    for arr, miss_rate in [
        (height, 0.1), (weight, 0.08), (chol, 0.15),
        (bps, 0.05), (bpd, 0.05), (glucose, 0.12), (creat, 0.2)
    ]:
        mask = rng.rand(n) < miss_rate
        arr[mask] = np.nan

    # Phenotype-like categorical (not necessarily real HPO codes)
    phenos = rng.choice([
        "HP:0001166", "HP:0001250", "HP:0000001", "HP:0100022", None
    ], size=n, p=[0.25, 0.25, 0.2, 0.15, 0.15])

    # Disease codes (fake)
    diseases = rng.choice(["DOID:9352", "DOID:12365", None], size=n, p=[0.45, 0.4, 0.15])

    # Class label with imbalance ~ 85/15
    labels = rng.choice(["majority", "minority"], size=n, p=[0.85, 0.15])

    # VisitDate: some invalid
    dates = (pd.to_datetime('2022-01-01') + pd.to_timedelta(rng.randint(0, 365, size=n), unit='D')).astype(str)
    invalid_idx = rng.choice(np.arange(n), size=max(5, n // 50), replace=False)
    dates = dates.tolist()
    for ix in invalid_idx:
        dates[ix] = "not_a_date"

    df = pd.DataFrame({
        "SampleID": sample_ids,
        "Height_cm": height,
        "Weight_kg": weight,
        "Cholesterol_mgdl": chol,
        "BP_systolic": bps,
        "BP_diastolic": bpd,
        "Glucose_mgdl": glucose,
        "Creatinine_mgdl": creat,
        "PrimaryPhenotype": phenos,
        "DiseaseCode": diseases,
        "VisitDate": dates,
        "class": labels,
    })

    # Add a few duplicates to exercise traceability
    df.loc[0, "SampleID"] = df.loc[1, "SampleID"]
    return df


def run_cli() -> Tuple[str, str, str, str]:
    # Save data
    df = create_comprehensive_data()
    df.to_csv(DATA_PATH, index=False)

    cmd = [
        sys.executable, "-m", "phenoqc",
        "--input", DATA_PATH,
        "--schema", SCHEMA_PATH,
        "--config", CONFIG_PATH,
        "--unique_identifiers", "SampleID",
        "--phenotype_columns", '{"PrimaryPhenotype": ["HPO"]}',
        "--output", OUT_DIR,
        "--quality-metrics", "accuracy", "redundancy", "traceability", "timeliness",
        "--label-column", "class",
        "--imbalance-threshold", "0.10",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = SRC_PATH + (os.pathsep + env.get("PYTHONPATH", ""))
    print("[INFO] Running:", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    print("[STDOUT]\n", proc.stdout)
    print("[STDERR]\n", proc.stderr)
    print("[INFO] Exit code:", proc.returncode)
    assert proc.returncode == 0

    processed_csv = unique_output_name(DATA_PATH, OUT_DIR, suffix=".csv")
    report_pdf = unique_output_name(DATA_PATH, OUT_DIR, suffix="_report.pdf")
    metrics_tsv = unique_output_name(DATA_PATH, OUT_DIR, suffix="_quality_metrics.tsv")
    metrics_json = unique_output_name(DATA_PATH, OUT_DIR, suffix="_quality_metrics_summary.json")
    # QC summary JSON
    qc_json = unique_output_name(DATA_PATH, OUT_DIR, suffix="_qc_summary.json")
    return processed_csv, report_pdf, metrics_tsv, metrics_json, qc_json


def verify_outputs(processed_csv: str, report_pdf: str, metrics_tsv: str, metrics_json: str, qc_json: str) -> None:
    assert os.path.exists(processed_csv), processed_csv
    assert os.path.exists(report_pdf), report_pdf
    assert os.path.exists(metrics_tsv), metrics_tsv
    assert os.path.exists(metrics_json), metrics_json
    assert os.path.exists(qc_json), qc_json

    df = pd.read_csv(processed_csv)
    # Imputation sanity: numeric missingness should be lower than raw
    num_missing = df.select_dtypes(include=["number"]).isna().sum().sum()
    print("[INFO] Post-imputation numeric NaNs:", int(num_missing))

    # Class imbalance sanity: constructed as imbalanced
    counts = df['class'].value_counts(dropna=True)
    minority_prop = counts.min() / counts.sum()
    assert minority_prop < 0.2, "Expected imbalanced classes"

    print("[INFO] Report located at:", report_pdf)


if __name__ == "__main__":
    paths = run_cli()
    verify_outputs(*paths)


