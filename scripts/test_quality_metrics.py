#!/usr/bin/env python3
"""
test_quality_metrics.py

A comprehensive test script for evaluating PhenoQC's quality-metrics flag
using a realistic dataset. It runs the PhenoQC CLI with the desired
metrics and verifies that each metric name appears in the stdout,
recording results in a summary CSV.

Example usage:
  python test_quality_metrics.py \
      --data examples/samples/sample_data.csv \
      --schema examples/schemas/pheno_schema.json \
      --config config.yaml \
      --unique_id SampleID \
      --output_dir ./test_results \
      --metrics accuracy redundancy traceability timeliness

The --metrics option accepts any combination of accuracy, redundancy,
traceability, and timeliness. Use "all" to enable every metric.
"""

import argparse
import os
import csv
import subprocess
from logging_module import setup_logging, log_activity

def run_phenoqc_cli(data_path, schema_path, config_path, unique_id,
                     output_dir, phenotype_columns, metrics):
    """Run the PhenoQC CLI with the provided parameters."""
    cmd = [
        "phenoqc",
        "--input", data_path,
        "--schema", schema_path,
        "--config", config_path,
        "--unique_identifiers", unique_id,
        "--phenotype_columns", phenotype_columns,
        "--output", output_dir,
        "--quality-metrics"
    ]
    cmd.extend(metrics)

    print(f"[INFO] Running PhenoQC with command:\n   {' '.join(cmd)}")
    proc = subprocess.run(cmd, capture_output=True, text=True)
    return proc.returncode, proc.stdout, proc.stderr

import re

def check_metrics_in_stdout(stdout_text, metrics):
    """Check whether each metric keyword appears in stdout as a whole word (case-insensitive)."""
    results = {}
    for metric in metrics:
        if metric.lower() == "all":
            continue
        # Use regex to match the metric as a whole word, case-insensitive
        pattern = r"\b{}\b".format(re.escape(metric))
        found = re.search(pattern, stdout_text, flags=re.IGNORECASE) is not None
        results[metric] = found
    return results

def write_summary(output_dir, summary_file, metrics_found, exit_code):
    """Append metric results to a CSV summary file."""
    os.makedirs(output_dir, exist_ok=True)
    summary_path = os.path.join(output_dir, summary_file)
    file_exists = os.path.isfile(summary_path)

    with open(summary_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if not file_exists:
            writer.writerow(["metric", "found_in_stdout", "exit_code"])
        for metric, found in metrics_found.items():
            writer.writerow([metric, found, exit_code])
    return summary_path

def main():
    parser = argparse.ArgumentParser(
        description="Run PhenoQC with --quality-metrics and verify output.")
    parser.add_argument("--data", required=True, help="Path to input data file.")
    parser.add_argument("--schema", required=True, help="Path to JSON schema file.")
    parser.add_argument("--config", required=True, help="Path to YAML config file.")
    parser.add_argument("--unique_id", required=True,
                        help="Column uniquely identifying each row.")
    parser.add_argument("--output_dir", default="./test_results",
                        help="Directory where PhenoQC outputs reports.")
    parser.add_argument(
        "--phenotype_columns",
        default='{"PrimaryPhenotype": ["HPO"]}',
        help="Phenotype columns mapping JSON string.")
    parser.add_argument(
        "--metrics",
        nargs="+",
        default=["all"],
        help="Quality metrics to evaluate (accuracy redundancy traceability timeliness or all).")
    parser.add_argument(
        "--summary_csv",
        default="quality_metrics_summary.csv",
        help="Filename for CSV summary results.")
    args = parser.parse_args()

    setup_logging(log_file="test_quality_metrics.log", mode="w")

    rc, stdout_text, stderr_text = run_phenoqc_cli(
        args.data, args.schema, args.config, args.unique_id,
        args.output_dir, args.phenotype_columns, args.metrics
    )

    print("\n[INFO] PhenoQC stdout:\n", stdout_text)
    print("\n[INFO] PhenoQC stderr:\n", stderr_text)

    metrics_to_check = args.metrics
    if any(m.lower() == "all" for m in metrics_to_check):
        metrics_to_check = ["accuracy", "redundancy", "traceability", "timeliness"]

    metrics_found = check_metrics_in_stdout(stdout_text, metrics_to_check)
    summary_path = write_summary(
        args.output_dir, args.summary_csv, metrics_found, rc
    )
    print(f"[INFO] Summary written to {summary_path}")

    if missing := [m for m, found in metrics_found.items() if not found]:
        print(f"[WARNING] Metrics missing from stdout: {missing}")
    else:
        print("[INFO] All requested metrics found in stdout.")

    if rc != 0:
        log_activity(f"PhenoQC exited with code {rc}", level="error")
    else:
        log_activity("PhenoQC completed successfully", level="info")

if __name__ == "__main__":
    main()
