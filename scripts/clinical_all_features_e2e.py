#!/usr/bin/env python3
"""
clinical_all_features_e2e.py

Comprehensive end-to-end script that:
- Generates a realistic clinical dataset with diverse data types
- Writes a full JSON Schema capturing types and constraints
- Writes a config.yaml covering all features:
  - Ontologies and fuzzy mapping config (threshold, cache expiry)
  - Imputation: global strategy, params, per-column overrides, tuning
  - Protected columns, redundancy settings
  - Quality metrics: accuracy, redundancy, traceability, timeliness,
    imputation_bias, imputation_stability, imputation_uncertainty
  - Class distribution
- Runs the PhenoQC CLI twice:
  1) Regular run
  2) Offline run to exercise cache/offline mapping path
- Verifies outputs exist and contain expected artifacts

Outputs are placed under scripts/output/clinical_all_features/

Usage:
  python scripts/clinical_all_features_e2e.py
"""

import os
import sys
import json
import subprocess
from typing import Tuple
import glob

import numpy as np
import pandas as pd


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

OUT_BASE = os.path.join(SCRIPT_DIR, "output", "clinical_all_features")
RUN1_DIR = os.path.join(OUT_BASE, "run_online")
RUN2_DIR = os.path.join(OUT_BASE, "run_offline")
os.makedirs(RUN1_DIR, exist_ok=True)
os.makedirs(RUN2_DIR, exist_ok=True)

DATA_PATH = os.path.join(OUT_BASE, "clinical_input.csv")
CONFIG_PATH = os.path.join(SCRIPT_DIR, "config", "clinical_all_features_config.yaml")
SCHEMA_PATH = os.path.join(SCRIPT_DIR, "config", "clinical_all_features_schema.json")
CUSTOM_MAPPING_PATH = os.path.join(SCRIPT_DIR, "config", "clinical_all_features_custom_mapping.json")
os.makedirs(os.path.join(SCRIPT_DIR, "config"), exist_ok=True)


def write_schema() -> None:
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "title": "Clinical All Features Schema",
        "type": "object",
        "properties": {
            "PatientID": {"type": ["string", "null"]},
            "VisitID": {"type": ["string", "null"]},
            "Age": {"type": ["number", "null"], "minimum": 0, "maximum": 120},
            "Sex": {"type": ["string", "null"], "enum": ["M", "F", "Other", None]},
            "Height_cm": {"type": ["number", "null"], "minimum": 30, "maximum": 250},
            "Weight_kg": {"type": ["number", "null"], "minimum": 1, "maximum": 400},
            "BMI": {"type": ["number", "null"], "minimum": 5, "maximum": 100},
            "BP_systolic": {"type": ["number", "null"], "minimum": 50, "maximum": 250},
            "BP_diastolic": {"type": ["number", "null"], "minimum": 30, "maximum": 200},
            "Cholesterol_mgdl": {"type": ["number", "null"], "minimum": 50, "maximum": 600},
            "Glucose_mgdl": {"type": ["number", "null"], "minimum": 20, "maximum": 1000},
            "Creatinine_mgdl": {"type": ["number", "null"], "minimum": 0.1, "maximum": 20},
            "PrimaryPhenotype": {"type": ["string", "null"]},
            "SecondaryPhenotype": {"type": ["string", "null"]},
            "DiseaseCode": {"type": ["string", "null"]},
            "MedicationCode": {"type": ["string", "null"]},
            "Smoker": {"type": ["boolean", "null"]},
            "Pregnant": {"type": ["boolean", "null"]},
            "VisitDate": {"type": ["string", "null"]},
            "class": {"type": ["string", "null"]}
        },
        "required": ["PatientID", "VisitID"],
    }
    with open(SCHEMA_PATH, "w", encoding="utf-8") as fh:
        json.dump(schema, fh, indent=2)


def write_config() -> None:
    cfg = f"""
ontologies:
  HPO:
    name: Human Phenotype Ontology
    source: local
    file: {os.path.join(PROJECT_ROOT, 'ontologies', 'hp.obo')}
    format: obo
  DO:
    name: Disease Ontology
    source: local
    file: {os.path.join(PROJECT_ROOT, 'ontologies', 'doid.obo')}
    format: obo
  MPO:
    name: Mammalian Phenotype Ontology
    source: local
    file: {os.path.join(PROJECT_ROOT, 'ontologies', 'mp.obo')}
    format: obo

default_ontologies:
  - HPO
  - DO
  - MPO

fuzzy_threshold: 82
cache_expiry_days: 1

imputation:
  strategy: knn
  params:
    n_neighbors: 5
    weights: uniform
    metric: nan_euclidean
  per_column:
    Age:
      strategy: mean
    Height_cm:
      strategy: median
    Weight_kg:
      strategy: knn
      params:
        n_neighbors: 7
        weights: distance
        metric: nan_euclidean
    BMI:
      strategy: mean
    BP_systolic:
      strategy: mice
      params:
        max_iter: 10
        random_state: 11
    BP_diastolic:
      strategy: mice
      params:
        max_iter: 10
        random_state: 11
    Glucose_mgdl:
      strategy: knn
      params:
        n_neighbors: 5
        weights: uniform
        metric: nan_euclidean
    Creatinine_mgdl:
      strategy: mice
      params:
        max_iter: 8
        random_state: 7
    Cholesterol_mgdl:
      strategy: svd
      params:
        rank: 3
        max_iters: 50
    Sex:
      strategy: mode
    Smoker:
      strategy: mode
    Pregnant:
      strategy: mode
  tuning:
    enable: true
    mask_fraction: 0.1
    scoring: MAE
    max_cells: 20000
    random_state: 42
    grid:
      n_neighbors: [3, 5, 7, 9]

protected_columns:
  - PatientID
  - VisitID
  - VisitDate
  - MedicationCode

redundancy:
  threshold: 0.98
  method: pearson

quality_metrics:
  imputation_bias:
    enable: true
    smd_threshold: 0.10
    var_ratio_low: 0.5
    var_ratio_high: 2.0
    ks_alpha: 0.05
  imputation_stability:
    enable: true
    repeats: 5
    mask_fraction: 0.1
    scoring: MAE

class_distribution:
  label_column: class
  warn_threshold: 0.10

mi_uncertainty:
  enable: true
  repeats: 3
  params:
    max_iter: 6
"""
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        fh.write(cfg)


def write_custom_mapping() -> None:
    # Pin a few terms explicitly to validate custom mapping path
    mapping = {
        "HP:0001250": {"HPO": "HP:0001250"},  # seizures as-is
        "seizure": {"HPO": "HP:0001250"},     # normalized synonym
        "DOID:9352": {"DO": "DOID:9352"}      # coronary artery disease
    }
    with open(CUSTOM_MAPPING_PATH, "w", encoding="utf-8") as fh:
        json.dump(mapping, fh, indent=2)


def create_dataset(n: int = 3000, seed: int = 11) -> pd.DataFrame:
    rng = np.random.RandomState(seed)
    pid = [f"P{str(i+1).zfill(6)}" for i in range(n)]
    vid = [f"V{str(i+1).zfill(6)}" for i in range(n)]

    age = rng.randint(0, 100, size=n).astype(float)
    sex = rng.choice(["M", "F", "Other", None], size=n, p=[0.48, 0.48, 0.02, 0.02])
    height = rng.normal(170, 10, n)
    weight = rng.normal(75, 15, n) + 0.15 * (height - 170)
    bmi = weight / ((height/100) ** 2)
    bp_sys = rng.normal(120, 18, n)
    bp_dia = rng.normal(78, 12, n)
    chol = np.clip(rng.lognormal(mean=5.1, sigma=0.35, size=n), 80, 600)
    gluc = np.clip(rng.normal(100, 25, n), 30, 1000)
    creat = np.abs(rng.normal(1.0, 0.4, n))

    # Missingness patterns
    for arr, miss in [
        (height, 0.10), (weight, 0.15), (bmi, 0.10), (bp_sys, 0.07), (bp_dia, 0.07), (chol, 0.12), (gluc, 0.12), (creat, 0.2), (age, 0.03)
    ]:
        mask = rng.rand(n) < miss
        arr[mask] = np.nan

    phenos1 = rng.choice(["HP:0001250", "HP:0001166", "HP:0000001", None], size=n, p=[0.25, 0.25, 0.25, 0.25])
    phenos2 = rng.choice(["HP:0100022", "HP:0002011", None], size=n, p=[0.3, 0.3, 0.4])
    diseases = rng.choice(["DOID:9352", "DOID:12365", None], size=n, p=[0.45, 0.4, 0.15])
    meds = rng.choice(["RXCUI:153666", "RXCUI:83367", None], size=n, p=[0.3, 0.3, 0.4])
    smoker = rng.choice([True, False, None], size=n, p=[0.25, 0.7, 0.05])
    pregnant = rng.choice([True, False, None], size=n, p=[0.02, 0.96, 0.02])

    base_date = pd.to_datetime('2022-01-01')
    dates = (base_date + pd.to_timedelta(rng.randint(0, 365, size=n), unit='D')).astype(str)
    dates = dates.to_numpy(copy=True)
    # inject some invalid dates
    for ix in rng.choice(np.arange(n), size=max(10, n // 300), replace=False):
        dates[ix] = "not_a_date"

    labels = rng.choice(["majority", "minority"], size=n, p=[0.88, 0.12])

    df = pd.DataFrame({
        "PatientID": pid,
        "VisitID": vid,
        "Age": age,
        "Sex": sex,
        "Height_cm": height,
        "Weight_kg": weight,
        "BMI": bmi,
        "BP_systolic": bp_sys,
        "BP_diastolic": bp_dia,
        "Cholesterol_mgdl": chol,
        "Glucose_mgdl": gluc,
        "Creatinine_mgdl": creat,
        "PrimaryPhenotype": phenos1,
        "SecondaryPhenotype": phenos2,
        "DiseaseCode": diseases,
        "MedicationCode": meds,
        "Smoker": smoker,
        "Pregnant": pregnant,
        "VisitDate": dates,
        "class": labels,
    })

    # Duplicates (traceability)
    if n >= 3:
        df.loc[1, ["PatientID", "VisitID"]] = df.loc[0, ["PatientID", "VisitID"]]
    return df


def unique_output_name(base_path: str, output_dir: str, suffix: str) -> str:
    from phenoqc.batch_processing import unique_output_name as _uon
    return _uon(base_path, output_dir, suffix=suffix)


def run_cli(output_dir: str, offline: bool = False) -> Tuple[str, str, str, str, str]:
    cmd = [
        sys.executable, "-m", "phenoqc",
        "--input", DATA_PATH,
        "--schema", SCHEMA_PATH,
        "--config", CONFIG_PATH,
        "--unique_identifiers", "PatientID", "VisitID",
        "--phenotype_columns", '{"PrimaryPhenotype": ["HPO"], "SecondaryPhenotype": ["HPO"], "DiseaseCode": ["DO"]}',
        "--custom_mappings", CUSTOM_MAPPING_PATH,
        "--output", output_dir,
        "--quality-metrics", "accuracy", "redundancy", "traceability", "timeliness", "imputation_bias",
        "--redundancy-threshold", "0.98",
        "--redundancy-method", "pearson",
        "--label-column", "class",
        "--imbalance-threshold", "0.10",
        "--bias-smd-threshold", "0.10",
        "--bias-var-low", "0.5",
        "--bias-var-high", "2.0",
        "--bias-ks-alpha", "0.05",
        "--impute-diagnostics", "on",
        "--mi-uncertainty", "on",
        "--mi-repeats", "3",
        "--mi-params", '{"max_iter": 6}',
    ]
    if offline:
        cmd.append("--offline")
    env = os.environ.copy()
    env["PYTHONPATH"] = SRC_PATH + (os.pathsep + env.get("PYTHONPATH", ""))
    print("[INFO] Running:", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    print("[STDOUT]\n", proc.stdout)
    print("[STDERR]\n", proc.stderr)
    print("[INFO] Exit code:", proc.returncode)
    assert proc.returncode == 0

    processed_csv = unique_output_name(DATA_PATH, output_dir, suffix=".csv")
    report_pdf = unique_output_name(DATA_PATH, output_dir, suffix="_report.pdf")
    metrics_tsv = unique_output_name(DATA_PATH, output_dir, suffix="_quality_metrics.tsv")
    metrics_json = unique_output_name(DATA_PATH, output_dir, suffix="_quality_metrics_summary.json")
    qc_json = unique_output_name(DATA_PATH, output_dir, suffix="_qc_summary.json")
    return processed_csv, report_pdf, metrics_tsv, metrics_json, qc_json


def _fallback_find(output_dir: str, base_name: str, suffix: str) -> str:
    """Find generated file by pattern if exact unique_output_name is missing."""
    name_no_ext, ext = os.path.splitext(os.path.basename(base_name))
    # Processed CSV example pattern: clinical_input_*_csv.csv
    if suffix == ".csv":
        pattern = os.path.join(output_dir, f"{name_no_ext}_*_csv.csv")
    elif suffix.endswith("_report.pdf"):
        pattern = os.path.join(output_dir, f"{name_no_ext}_*{suffix}")
    elif suffix.endswith("_quality_metrics.tsv"):
        pattern = os.path.join(output_dir, f"{name_no_ext}_*{suffix}")
    elif suffix.endswith("_quality_metrics_summary.json"):
        pattern = os.path.join(output_dir, f"{name_no_ext}_*{suffix}")
    elif suffix.endswith("_qc_summary.json"):
        pattern = os.path.join(output_dir, f"{name_no_ext}_*{suffix}")
    else:
        pattern = os.path.join(output_dir, f"{name_no_ext}_*{suffix}")
    matches = sorted(glob.glob(pattern))
    return matches[0] if matches else ""


def verify_outputs(processed_csv: str, report_pdf: str, metrics_tsv: str, metrics_json: str, qc_json: str) -> dict:
    # Fallback discovery if direct paths are missing
    out_dir = os.path.dirname(processed_csv)
    base_path = processed_csv.replace("_csv.csv", ".csv") if processed_csv.endswith("_csv.csv") else processed_csv
    if not os.path.exists(processed_csv):
        candidate = _fallback_find(out_dir, base_path, ".csv")
        assert candidate, processed_csv
        processed_csv = candidate
    if not os.path.exists(report_pdf):
        candidate = _fallback_find(out_dir, base_path, "_report.pdf")
        assert candidate, report_pdf
        report_pdf = candidate
    if not os.path.exists(metrics_tsv):
        candidate = _fallback_find(out_dir, base_path, "_quality_metrics.tsv")
        assert candidate, metrics_tsv
        metrics_tsv = candidate
    if not os.path.exists(metrics_json):
        candidate = _fallback_find(out_dir, base_path, "_quality_metrics_summary.json")
        assert candidate, metrics_json
        metrics_json = candidate
    if not os.path.exists(qc_json):
        candidate = _fallback_find(out_dir, base_path, "_qc_summary.json")
        assert candidate, qc_json
        qc_json = candidate

    df = pd.read_csv(processed_csv)
    # Expect mapping outputs and missing flag
    assert "HPO_ID" in df.columns or "DO_ID" in df.columns
    assert "MissingDataFlag" in df.columns

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
    assert len(bias_rows) >= 1

    # Optional stability rows presence check
    stab_rows = (
        qc.get("quality_metrics", {})
          .get("imputation_stability", {})
          .get("rows", [])
    )
    assert isinstance(stab_rows, list)
    return qc


def main() -> None:
    df = create_dataset()
    df.to_csv(DATA_PATH, index=False)
    write_schema()
    write_config()
    write_custom_mapping()

    # Run 1: online (default)
    r1 = run_cli(RUN1_DIR, offline=False)
    qc1 = verify_outputs(*r1)

    # Run 2: offline to exercise cache/local mapping
    r2 = run_cli(RUN2_DIR, offline=True)
    qc2 = verify_outputs(*r2)

    # If class distribution present, ensure same structure
    assert isinstance(qc1.get("class_distribution"), (dict, type(None)))
    assert isinstance(qc2.get("class_distribution"), (dict, type(None)))

    print("[SUCCESS] Clinical all-features E2E completed. Outputs in:", OUT_BASE)


if __name__ == "__main__":
    main()


