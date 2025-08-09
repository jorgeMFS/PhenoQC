#!/usr/bin/env python3
"""
super_comprehensive_cli_test.py

Generates a large, heterogeneous dataset with extensive edge cases, writes a
dedicated schema and config, runs the PhenoQC CLI (module mode), and verifies
all expected artifacts including the QC JSON with imputation-bias diagnostics.

Usage:
  python scripts/super_comprehensive_cli_test.py
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

OUT_DIR = os.path.join(SCRIPT_DIR, "output", "super_comprehensive")
os.makedirs(OUT_DIR, exist_ok=True)

DATA_PATH = os.path.join(OUT_DIR, "super_input.csv")
SCHEMA_PATH = os.path.join(SCRIPT_DIR, "config", "super_schema.json")
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config", "super_config.yaml")


def write_schema() -> None:
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Super Comprehensive Schema",
        "type": "object",
        "properties": {
            "SampleID": {"type": ["string", "null"]},
            "Height_cm": {"type": ["number", "null"], "minimum": 30, "maximum": 250},
            "Weight_kg": {"type": ["number", "null"], "minimum": 1, "maximum": 400},
            "Cholesterol_mgdl": {"type": ["number", "null"], "minimum": 50, "maximum": 600},
            "BP_systolic": {"type": ["number", "null"], "minimum": 50, "maximum": 250},
            "BP_diastolic": {"type": ["number", "null"], "minimum": 30, "maximum": 200},
            "Glucose_mgdl": {"type": ["number", "null"], "minimum": 20, "maximum": 800},
            "Creatinine_mgdl": {"type": ["number", "null"], "minimum": 0.1, "maximum": 20},
            "PrimaryPhenotype": {"type": ["string", "null"]},
            "DiseaseCode": {"type": ["string", "null"]},
            "VisitDate": {"type": ["string", "null"]},
            "class": {"type": ["string", "null"]}
        },
        "required": ["SampleID"],
    }
    os.makedirs(os.path.join(SCRIPT_DIR, "config"), exist_ok=True)
    with open(SCHEMA_PATH, "w", encoding="utf-8") as fh:
        json.dump(schema, fh, indent=2)


def write_config() -> None:
    cfg = """
ontologies:
  HPO:
    name: Human Phenotype Ontology
    source: url
    url: http://purl.obolibrary.org/obo/hp.obo
    format: obo
  DO:
    name: Disease Ontology
    source: url
    url: http://purl.obolibrary.org/obo/doid.obo
    format: obo
  MPO:
    name: Mammalian Phenotype Ontology
    source: url
    url: http://purl.obolibrary.org/obo/mp.obo
    format: obo

default_ontologies:
  - HPO
  - DO
  - MPO

fuzzy_threshold: 80
cache_expiry_days: 30

imputation:
  strategy: knn
  params:
    n_neighbors: 5
    weights: uniform
  per_column:
    Creatinine_mgdl:
      strategy: mice
      params:
        max_iter: 5
    Cholesterol_mgdl:
      strategy: svd
      params:
        rank: 2
  tuning:
    enable: true
    mask_fraction: 0.1
    scoring: MAE
    max_cells: 10000
    random_state: 42
    grid:
      n_neighbors: [3, 5, 7]

quality_metrics:
  imputation_bias:
    enable: true
    smd_threshold: 0.10
    var_ratio_low: 0.5
    var_ratio_high: 2.0
    ks_alpha: 0.05
"""
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        fh.write(cfg)


def create_super_comprehensive_data(n: int = 5000, seed: int = 7) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    sample_ids = [f"S{str(i+1).zfill(6)}" for i in range(n)]

    # Numeric core with different distributions and heteroscedasticity
    height = rng.normal(170, 10, n)
    weight = rng.normal(70, 12, n) + 0.2 * (height - 170)
    chol = np.clip(rng.lognormal(mean=5.2, sigma=0.3, size=n), 80, 500)
    bps = rng.normal(120, 15, n)
    bpd = rng.normal(80, 12, n)
    glucose = rng.normal(95, 20, n)
    creat = np.abs(rng.normal(1.0, 0.3, n))

    # Introduce structured missingness
    for arr, miss_rate in [
        (height, 0.08), (weight, 0.15), (chol, 0.10),
        (bps, 0.05), (bpd, 0.05), (glucose, 0.12), (creat, 0.20)
    ]:
        mask = rng.rand(n) < miss_rate
        arr[mask] = np.nan

    # Outliers & anomalies
    out_idx = rng.choice(n, size=max(5, n // 100), replace=False)
    chol[out_idx] = chol[out_idx] * 1.8
    glucose[rng.choice(n, size=max(3, n // 200), replace=False)] = 900  # extreme

    # Phenotypes & diseases (categorical)
    phenos = rng.choice([
        "HP:0001166", "HP:0001250", "HP:0000001", "HP:0100022", None
    ], size=n, p=[0.25, 0.25, 0.2, 0.15, 0.15])
    diseases = rng.choice(["DOID:9352", "DOID:12365", None], size=n, p=[0.45, 0.4, 0.15])

    # Dates: valid and invalid
    base_date = pd.to_datetime('2021-01-01')
    dates = (base_date + pd.to_timedelta(rng.randint(0, 365*2, size=n), unit='D')).astype(str)
    for ix in rng.choice(np.arange(n), size=max(10, n // 200), replace=False):
        dates[ix] = "not_a_date"

    # Labels with strong imbalance
    labels = rng.choice(["majority", "minority"], size=n, p=[0.88, 0.12])

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

    # Duplicates across rows for traceability check
    if n >= 3:
        df.loc[1, "SampleID"] = df.loc[0, "SampleID"]
    return df


def unique_output_name(base_path: str, output_dir: str, suffix: str) -> str:
    from phenoqc.batch_processing import unique_output_name as _uon
    return _uon(base_path, output_dir, suffix=suffix)


def run_cli() -> Tuple[str, str, str, str, str]:
    df = create_super_comprehensive_data()
    df.to_csv(DATA_PATH, index=False)
    write_schema()
    write_config()

    cmd = [
        sys.executable, "-m", "phenoqc",
        "--input", DATA_PATH,
        "--schema", SCHEMA_PATH,
        "--config", CONFIG_PATH,
        "--unique_identifiers", "SampleID",
        "--phenotype_columns", '{"PrimaryPhenotype": ["HPO"], "DiseaseCode": ["DO"]}',
        "--output", OUT_DIR,
        "--quality-metrics", "accuracy", "redundancy", "traceability", "timeliness", "imputation_bias",
        "--label-column", "class",
        "--imbalance-threshold", "0.10",
        "--bias-smd-threshold", "0.10",
        "--bias-var-low", "0.5",
        "--bias-var-high", "2.0",
        "--bias-ks-alpha", "0.05",
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
    qc_json = unique_output_name(DATA_PATH, OUT_DIR, suffix="_qc_summary.json")
    return processed_csv, report_pdf, metrics_tsv, metrics_json, qc_json


def verify_outputs(processed_csv: str, report_pdf: str, metrics_tsv: str, metrics_json: str, qc_json: str) -> None:
    assert os.path.exists(processed_csv), processed_csv
    assert os.path.exists(report_pdf), report_pdf
    assert os.path.exists(metrics_tsv), metrics_tsv
    assert os.path.exists(metrics_json), metrics_json
    assert os.path.exists(qc_json), qc_json

    # Basic sanity on processed CSV
    df = pd.read_csv(processed_csv)
    assert "HPO_ID" in df.columns or "DO_ID" in df.columns
    assert "MissingDataFlag" in df.columns

    # Validate QC JSON contents
    with open(qc_json, "r", encoding="utf-8") as fh:
        qc = json.load(fh)
    assert "quality_scores" in qc and isinstance(qc["quality_scores"], dict)
    assert "imputation" in qc and isinstance(qc["imputation"], dict)
    # Bias diagnostic rows
    bias_rows = (
        qc.get("quality_metrics", {})
          .get("imputation_bias", {})
          .get("rows", [])
    )
    assert isinstance(bias_rows, list)
    # Expect at least one evaluated variable when enabled
    assert len(bias_rows) >= 1


if __name__ == "__main__":
    paths = run_cli()
    verify_outputs(*paths)


