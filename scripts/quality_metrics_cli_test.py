#!/usr/bin/env python3
"""
quality_metrics_cli_test.py

A self-contained script demonstrating PhenoQC's ``--quality-metrics`` option.
It synthesizes a small dataset containing issues for all supported metrics
(accuracy, redundancy, traceability, timeliness) and runs the CLI with those
checks enabled.

Example:
    python quality_metrics_cli_test.py

Outputs and temporary files are written under ``./output/quality_metrics``.
"""

import os
import sys
import subprocess
from datetime import datetime, timedelta

import pandas as pd
import yaml

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from phenoqc.quality_metrics import QUALITY_METRIC_CHOICES
from phenoqc.batch_processing import unique_output_name
BASE_CONFIG = os.path.join(SCRIPT_DIR, "config", "config.yaml")
SCHEMA_PATH = os.path.join(SCRIPT_DIR, "config", "schema.json")


def create_test_data(csv_path: str) -> None:
    """Generate a small CSV with deliberate quality issues."""
    now = datetime.now()
    old_date = (now - timedelta(days=10)).strftime("%Y-%m-%d")
    df = pd.DataFrame(
        {
            "SampleID": [1, 1, None],  # duplicate and missing for traceability
            "Height_cm": [170, -5, 180],  # negative value breaks accuracy
            "Weight_kg": [70, 80, 75],
            # identical to Height_cm to trigger redundancy detection
            "Cholesterol_mgdl": [170, -5, 180],
            "BP_systolic": [120, 100, 140],
            "BP_diastolic": [60, 50, 70],
            "Glucose_mgdl": [90, 95, 100],
            "Creatinine_mgdl": [1.0, 1.2, 1.1],
            "PrimaryPhenotype": ["HP:0001250", "HP:0001166", "HP:0000001"],
            # old, current, and invalid dates for timeliness
            "VisitDate": [old_date, now.strftime("%Y-%m-%d"), "not_a_date"],
        }
    )
    df.to_csv(csv_path, index=False)


def create_quality_config(cfg_path: str) -> None:
    """Write a config file enabling timeliness checks."""
    with open(BASE_CONFIG, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)
    cfg.update({"date_col": "VisitDate", "max_lag_days": 5})
    with open(cfg_path, "w", encoding="utf-8") as fh:
        yaml.safe_dump(cfg, fh)


def run_phenoqc(data_path: str, cfg_path: str, output_dir: str) -> None:
    """Execute the PhenoQC CLI with all quality metrics enabled."""
    cmd = [
        sys.executable,
        "-m",
        "phenoqc",
        "--input",
        data_path,
        "--schema",
        SCHEMA_PATH,
        "--config",
        cfg_path,
        "--unique_identifiers",
        "SampleID",
        "--phenotype_columns",
        '{"PrimaryPhenotype": ["HPO"]}',
        "--output",
        output_dir,
        "--quality-metrics",
    ] + QUALITY_METRIC_CHOICES
    print("[INFO] Running:", " ".join(cmd))
    env = os.environ.copy()
    # Ensure the module can be resolved when running as -m
    env["PYTHONPATH"] = SRC_PATH + (os.pathsep + env["PYTHONPATH"] if "PYTHONPATH" in env and env["PYTHONPATH"] else "")
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    print("[STDOUT]\n", proc.stdout)
    print("[STDERR]\n", proc.stderr)
    print("[INFO] Exit code:", proc.returncode)

    # Assert exit code is 0
    assert proc.returncode == 0, f"CLI exited with non-zero code: {proc.returncode}"

    # Assert expected output artifacts created by the CLI
    processed_csv = unique_output_name(data_path, output_dir, suffix=".csv")
    report_pdf = unique_output_name(data_path, output_dir, suffix="_report.pdf")

    assert os.path.exists(processed_csv), f"Processed CSV not found: {processed_csv}"
    assert os.path.exists(report_pdf), f"Report PDF not found: {report_pdf}"

    # The PDF should not be empty
    assert os.path.getsize(report_pdf) > 0, f"Report PDF is empty: {report_pdf}"

    # Basic sanity check on processed CSV columns (HPO mapping should produce HPO_ID)
    import pandas as pd
    df = pd.read_csv(processed_csv)
    assert "HPO_ID" in df.columns, "Expected 'HPO_ID' column in processed CSV after mapping"

    # Verify per-file quality metrics artifacts exist
    metrics_tsv = unique_output_name(data_path, output_dir, suffix="_quality_metrics.tsv")
    metrics_json = unique_output_name(data_path, output_dir, suffix="_quality_metrics_summary.json")
    assert os.path.exists(metrics_tsv), f"Quality metrics TSV not found: {metrics_tsv}"
    assert os.path.exists(metrics_json), f"Quality metrics summary JSON not found: {metrics_json}"

    # Validate JSON counts vs TSV rows per metric
    import json
    import pandas as pd
    with open(metrics_json, "r", encoding="utf-8") as jf:
        summary = json.load(jf)
    tsv_df = pd.read_csv(metrics_tsv, sep="\t") if os.path.getsize(metrics_tsv) > 0 else pd.DataFrame()
    if not tsv_df.empty and "metric" in tsv_df.columns:
        for m in ["accuracy", "redundancy", "traceability", "timeliness"]:
            if m in summary:
                # Count rows for this metric in TSV
                tsv_count = int((tsv_df["metric"] == m).sum())
                json_count = int(summary.get(m, 0))
                assert tsv_count == json_count, f"Mismatch for {m}: TSV={tsv_count}, JSON={json_count}"


def main() -> None:
    out_dir = os.path.join(SCRIPT_DIR, "output", "quality_metrics")
    os.makedirs(out_dir, exist_ok=True)

    data_path = os.path.join(out_dir, "quality_metrics_input.csv")
    cfg_path = os.path.join(out_dir, "quality_metrics_config.yaml")

    create_test_data(data_path)
    create_quality_config(cfg_path)
    run_phenoqc(data_path, cfg_path, out_dir)


if __name__ == "__main__":
    main()
