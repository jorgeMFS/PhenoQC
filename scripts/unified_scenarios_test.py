#!/usr/bin/env python3
"""
unified_scenarios_test.py

One script to run multiple synthetic-data scenarios and measure PhenoQC performance.

Scenarios implemented:
  1) SCALING           (varying num_records)
  2) MISSINGNESS       (varying missing_rate)
  3) CORRUPTION        (varying invalid_rate)
  4) DUPLICATE_STRESS  (varying duplicate_rate)

Additionally:
 - Measures both runtime and CPU usage for each step (data generation + benchmark).
 - Saves a single summary CSV at the end, including shape, duplicates found, schema violations, missing data, etc.

Requires:
 - generate_synthetic_ontology_data.py  (the data generator)
 - phenoqc_benchmark.py                 (the QC runner)
 - psutil (for CPU usage) - optional, see instructions below.

Usage:
  python unified_scenarios_test.py

If you do not want to rely on `psutil`, you can either remove the CPU usage parts or wrap them in try/except.
"""

import os
import subprocess
import csv
import time
import re


try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("[WARNING] psutil not found; CPU usage measurement will be skipped.")


# =========================================
# CONFIG: paths to your existing scripts
# =========================================
# Use paths relative to repo root (this file lives under scripts/)
GENERATE_SCRIPT = "scripts/generate_synthetic_ontology_data.py"
BENCHMARK_SCRIPT = "scripts/phenoqc_benchmark.py"

# Where outputs go
OUTPUT_DIR = "scripts/unified_scenarios_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Common config, schema, etc.
CONFIG_PATH = "scripts/config/config.yaml"
SCHEMA_PATH = "scripts/config/schema.json"

# ==================================================================
# SCENARIO DEFINITIONS
# Each item is a dict specifying:
#   "name": scenario identifier
#   "param_name": the generator param to vary (e.g. "num_records")
#   "param_values": list of values to test
#   "fixed_params": any other generator settings to keep constant
#   "desc": optional description
# ===================================================================
SCENARIOS = [
    {
        "name": "SCALING",
        "desc": "Vary dataset size to test performance scaling.",
        "param_name": "num_records",
        "param_values": [10000, 50000, 100000],
        "fixed_params": {
            "missing_rate": 0.1,
            "duplicate_rate": 0.05,
            "invalid_rate": 0.1
        }
    },
    {
        "name": "MISSINGNESS",
        "desc": "Vary missing_rate for the same dataset size to see post-imputation changes.",
        "param_name": "missing_rate",
        "param_values": [0.05, 0.2, 0.5],
        "fixed_params": {
            "num_records": 3000,
            "duplicate_rate": 0.05,
            "invalid_rate": 0.1
        }
    },
    {
        "name": "CORRUPTION",
        "desc": "Vary invalid_rate (ontology corruption).",
        "param_name": "invalid_rate",
        "param_values": [0.05, 0.2, 0.5],
        "fixed_params": {
            "num_records": 3000,
            "missing_rate": 0.1,
            "duplicate_rate": 0.05
        }
    },
    {
        "name": "DUPLICATE_STRESS",
        "desc": "Test 20% or 30% duplicates",
        "param_name": "duplicate_rate",
        "param_values": [0.20, 0.30],
        "fixed_params": {
            "num_records": 3000,
            "missing_rate": 0.1,
            "invalid_rate": 0.1
        }
    }
]


def measure_cpu_usage_start():
    """
    Returns a psutil.Process object so we can measure CPU usage delta.
    If psutil not installed, returns None.
    """
    if not PSUTIL_AVAILABLE:
        return None
    return psutil.Process(os.getpid())


def measure_cpu_usage_end(process_obj, start_cpu_times):
    """
    Given the psutil.Process object and the CPU times at start,
    returns the user/system CPU seconds consumed as a tuple.
    If not available, returns None.
    """
    if not PSUTIL_AVAILABLE or process_obj is None:
        return None

    end_cpu_times = process_obj.cpu_times()
    cpu_user_delta = end_cpu_times.user - start_cpu_times.user
    cpu_sys_delta = end_cpu_times.system - start_cpu_times.system
    return (cpu_user_delta, cpu_sys_delta)


def generate_data(
    num_records=3000,
    missing_rate=0.1,
    duplicate_rate=0.05,
    invalid_rate=0.1,
    hpo_samples=3000,
    do_samples=3000,
    mpo_samples=3000,
    output_prefix="scenario"
):
    """
    Calls generate_synthetic_ontology_data.py with the given parameters.
    Returns (data_csv, ground_truth_csv, generation_time, generation_cpu_usage, success_bool).
    """
    data_csv = f"{output_prefix}_data.csv"
    gt_csv = f"{output_prefix}_ground_truth.csv"

    gen_cmd = [
        "python", GENERATE_SCRIPT,
        "--config", CONFIG_PATH,
        "--num_records", str(num_records),
        "--missing_rate", str(missing_rate),
        "--duplicate_rate", str(duplicate_rate),
        "--invalid_rate", str(invalid_rate),
        "--hpo_samples", str(hpo_samples),
        "--do_samples", str(do_samples),
        "--mpo_samples", str(mpo_samples),
        "--output_prefix", output_prefix
    ]

    print("[INFO] Generating synthetic data:\n ", " ".join(gen_cmd))

    start_time = time.time()
    process_obj = measure_cpu_usage_start()
    start_cpu_times = process_obj.cpu_times() if process_obj else None

    proc = subprocess.run(gen_cmd, capture_output=True, text=True)
    elapsed = time.time() - start_time
    cpu_result = measure_cpu_usage_end(process_obj, start_cpu_times)

    if proc.returncode != 0:
        print("[ERROR] Generator failed. Stderr:")
        print(proc.stderr)
        return data_csv, gt_csv, elapsed, cpu_result, False
    else:
        print(f"[INFO] Generation took {elapsed:.2f}s. stdout:\n", proc.stdout)
        return data_csv, gt_csv, elapsed, cpu_result, True


def run_phenoqc_benchmark(
    data_csv,
    output_dir=OUTPUT_DIR,
    impute_strategy="mean",
    phenotype_column="PrimaryPhenotype",
    unique_ids=["SampleID"],
    ontologies=None,
    recursive=False,
    columns_to_check=None
):
    """
    Calls phenoqc_benchmark.py with the given arguments.
    Returns (benchmark_time, benchmark_cpu_usage, ret_code, stdout, stderr).
    """
    bench_cmd = [
        "python", BENCHMARK_SCRIPT,
        "--input_data", data_csv,
        "--config", CONFIG_PATH,
        "--schema", SCHEMA_PATH,
        "--output_dir", output_dir,
        "--uniqueIDs"
    ] + unique_ids + [
        "--impute_strategy", impute_strategy,
        "--phenotype_column", phenotype_column
    ]

    if ontologies:
        bench_cmd.append("--ontologies")
        bench_cmd.extend(ontologies)

    if recursive:
        bench_cmd.append("--recursive")

    if columns_to_check and len(columns_to_check) > 0:
        bench_cmd.append("--check_columns")
        bench_cmd.extend(columns_to_check)

    print("\n[INFO] Running phenoqc_benchmark:\n ", " ".join(bench_cmd))

    start_time = time.time()
    process_obj = measure_cpu_usage_start()
    start_cpu_times = process_obj.cpu_times() if process_obj else None

    proc = subprocess.run(bench_cmd, capture_output=True, text=True)
    elapsed = time.time() - start_time
    cpu_result = measure_cpu_usage_end(process_obj, start_cpu_times)

    print("[INFO] phenoqc_benchmark stdout:\n", proc.stdout)
    print("[INFO] phenoqc_benchmark stderr:\n", proc.stderr)
    print(f"[INFO] Benchmark took {elapsed:.2f}s")

    return elapsed, cpu_result, proc.returncode, proc.stdout, proc.stderr


def parse_processed_shape(benchmark_stdout):
    """
    Finds a line like "Processed CSV shape: (3000, 21)" and returns an (rows,cols) tuple if possible.
    Otherwise returns (None, None).
    """
    pattern = re.compile(r"Processed CSV shape:\s*\((\d+),\s*(\d+)\)")
    match = pattern.search(benchmark_stdout)
    if match:
        rows = int(match.group(1))
        cols = int(match.group(2))
        return rows, cols
    return None, None


def parse_duplicates(benchmark_stdout):
    """
    Finds a line like "[WARNING] Found 300 duplicates in final CSV..." and returns the integer 300.
    If not found, returns 0.
    """
    pattern = re.compile(r"Found\s+(\d+)\s+duplicates\s+in\s+final\s+CSV", re.IGNORECASE)
    match = pattern.search(benchmark_stdout)
    if match:
        return int(match.group(1))
    return 0


def parse_schema_violations(benchmark_stdout):
    """
    E.g. "Format validation failed. 6012 record(s) do not match the JSON schema"
    Returns integer 6012 if found, else 0.
    """
    pattern = re.compile(r"Format validation failed\.\s+(\d+)\s+record\(s\)", re.IGNORECASE)
    match = pattern.search(benchmark_stdout)
    if match:
        return int(match.group(1))
    return 0


def parse_missing_data(benchmark_stdout):
    """
    OPTIONAL: parse final missing data lines from the phenoqc_benchmark output.
    e.g.:
      "[INFO] Missing data (post-imputation) in selected columns:"
       "Height_cm: missing=12 (0.40%)"
    We'll just parse the overall summary into a single string or dictionary. 

    Here, we do something basic: return the entire block as a single string.
    If you want per-column details, you can parse them line by line and store them in a dict.
    """
    lines = benchmark_stdout.splitlines()
    collecting = False
    collected = []
    for line in lines:
        if "Missing data (post-imputation) in selected columns:" in line:
            collecting = True
            continue
        if collecting:
            # if we see a blank line or something else, we might stop collecting.
            # For now, let's stop collecting if we see "[INFO]" or something
            if line.strip().startswith("[INFO]") or line.strip().startswith("[WARNING]") or "duplicates" in line:
                # stop
                break
            collected.append(line.strip())

    # Return as a single multiline string
    return "\n".join(collected).strip() if collected else ""


def main():
    results = []
    scenario_csv = os.path.join(OUTPUT_DIR, "unified_scenarios_summary.csv")

    for scenario in SCENARIOS:
        scenario_name = scenario["name"]
        param_name = scenario["param_name"]   # e.g. "num_records" or "missing_rate" or "invalid_rate"
        param_values = scenario["param_values"]
        fixed_params = scenario["fixed_params"]

        print(f"\n=== Running scenario {scenario_name} : {scenario.get('desc','')} ===\n")

        for val in param_values:
            # 1) Construct the output prefix
            prefix = os.path.join(OUTPUT_DIR, f"{scenario_name.lower()}_{param_name}_{val}")
            # 2) Merge the fixed params + param_name=val
            gen_args = {
                "num_records": fixed_params.get("num_records", 3000),
                "missing_rate": fixed_params.get("missing_rate", 0.1),
                "duplicate_rate": fixed_params.get("duplicate_rate", 0.05),
                "invalid_rate": fixed_params.get("invalid_rate", 0.1),
                "output_prefix": prefix
            }
            # Overwrite the main param
            gen_args[param_name] = val

            # 3) Generate data
            data_csv, gt_csv, gen_time, gen_cpu_usage, success = generate_data(**gen_args)
            # gen_cpu_usage => tuple (user_delta, sys_delta) or None
            if not success:
                row = {
                    "Scenario": scenario_name,
                    "Parameter": f"{param_name}={val}",
                    "GenTime(s)": f"{gen_time:.2f}",
                    "GenCPU(user,sys)": str(gen_cpu_usage) if gen_cpu_usage else "",
                    "BenchTime(s)": "",
                    "BenchCPU(user,sys)": "",
                    "ReturnCode": -1,
                    "Rows": "",
                    "Cols": "",
                    "DuplicatesFound": "",
                    "SchemaViolations": "",
                    "MissingDataSummary": ""
                }
                results.append(row)
                continue

            # 4) Run benchmark
            bench_time, bench_cpu_usage, ret_code, bench_stdout, bench_stderr = run_phenoqc_benchmark(
                data_csv=data_csv,
                output_dir=OUTPUT_DIR,
                impute_strategy="mean",
                phenotype_column="PrimaryPhenotype",
                unique_ids=["SampleID"],
                ontologies=None,
                recursive=False,
                columns_to_check=["Height_cm","Weight_kg","Cholesterol_mgdl"] 
            )

            # parse shape => (rows, cols)
            rows, cols = parse_processed_shape(bench_stdout)
            # parse duplicates => integer
            dup_count = parse_duplicates(bench_stdout)
            # parse schema violations => integer
            schema_violations = parse_schema_violations(bench_stdout)
            # parse missing data => short text
            missing_data_summary = parse_missing_data(bench_stdout)

            # 6) Collect result row
            row = {
                "Scenario": scenario_name,
                "Parameter": f"{param_name}={val}",
                "GenTime(s)": f"{gen_time:.2f}",
                "GenCPU(user,sys)": str(gen_cpu_usage) if gen_cpu_usage else "",
                "BenchTime(s)": f"{bench_time:.2f}",
                "BenchCPU(user,sys)": str(bench_cpu_usage) if bench_cpu_usage else "",
                "ReturnCode": ret_code,
                "Rows": str(rows) if rows else "",
                "Cols": str(cols) if cols else "",
                "DuplicatesFound": str(dup_count),
                "SchemaViolations": str(schema_violations),
                "MissingDataSummary": missing_data_summary.replace('\n','; ')
            }
            results.append(row)

    # Write out final summary CSV
    scenario_csv = os.path.join(OUTPUT_DIR, "unified_scenarios_summary.csv")
    with open(scenario_csv, "w", newline="") as f:
        fieldnames = [
            "Scenario","Parameter","GenTime(s)","GenCPU(user,sys)",
            "BenchTime(s)","BenchCPU(user,sys)",
            "ReturnCode","Rows","Cols","DuplicatesFound","SchemaViolations","MissingDataSummary"
        ]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"\n=== All scenarios complete! Summary saved to {scenario_csv} ===")
    if not PSUTIL_AVAILABLE:
        print("[INFO] If you install psutil, the CPU usage columns will become meaningful.")


if __name__ == "__main__":
    main()
