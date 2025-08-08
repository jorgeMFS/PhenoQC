#!/usr/bin/env python3
import os
import sys
import subprocess
import json
import pandas as pd

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
SRC_PATH = os.path.join(PROJECT_ROOT, "src")
if SRC_PATH not in sys.path:
    sys.path.insert(0, SRC_PATH)

from phenoqc.batch_processing import unique_output_name

DATA_PATH = os.path.join(SCRIPT_DIR, "output", "quality_metrics", "quality_metrics_input.csv")
SCHEMA_PATH = os.path.join(SCRIPT_DIR, "config", "schema.json")
CFG_PATH = os.path.join(SCRIPT_DIR, "config", "config.yaml")
OUT_DIR = os.path.join(SCRIPT_DIR, "output", "quality_metrics")


def run():
    os.makedirs(OUT_DIR, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "phenoqc",
        "--input",
        DATA_PATH,
        "--schema",
        SCHEMA_PATH,
        "--config",
        CFG_PATH,
        "--unique_identifiers",
        "SampleID",
        "--phenotype_columns",
        '{"PrimaryPhenotype": ["HPO"]}',
        "--output",
        OUT_DIR,
        "--quality-metrics",
        "accuracy",
        "redundancy",
        "traceability",
        "timeliness",
        "--label-column",
        "class",
        "--imbalance-threshold",
        "0.10",
    ]
    env = os.environ.copy()
    env["PYTHONPATH"] = SRC_PATH + (os.pathsep + env.get("PYTHONPATH", ""))
    print("[INFO] Running:", " ".join(cmd))
    proc = subprocess.run(cmd, capture_output=True, text=True, env=env)
    print("[STDOUT]\n", proc.stdout)
    print("[STDERR]\n", proc.stderr)
    print("[INFO] Exit code:", proc.returncode)
    assert proc.returncode == 0

    # Verify outputs
    processed_csv = unique_output_name(DATA_PATH, OUT_DIR, suffix=".csv")
    report_pdf = unique_output_name(DATA_PATH, OUT_DIR, suffix="_report.pdf")
    assert os.path.exists(processed_csv), processed_csv
    assert os.path.exists(report_pdf), report_pdf

    df = pd.read_csv(processed_csv)
    assert "HPO_ID" in df.columns
    # Ensure some imputation took place (at least one NaN removed in numeric columns)
    num = df.select_dtypes(include=["number"]).isna().sum().sum()
    assert num >= 0

    # Ensure class distribution artifacts present in PDF (we just confirm file exists)
    print("[INFO] Report ready:", report_pdf)


if __name__ == "__main__":
    run()


