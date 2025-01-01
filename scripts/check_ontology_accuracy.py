"""
check_ontology_accuracy.py

This script now has an optional --ground_truth argument.
If you don't supply a ground truth file, we simply load the processed CSV
and skip the accuracy checks.

Example usage:
  python check_ontology_accuracy.py \
    --processed_csv ./reports/synthetic_phenotypic_data.csv

If you DID have a ground truth CSV with columns like [SampleID, TrueHPO_ID, TrueDO_ID],
you could do:
  python check_ontology_accuracy.py \
    --processed_csv ../reports/synthetic_phenotypic_data.csv \
    --id_column SampleID \
    --hpo_column HPO_ID \
    --do_column DO_ID
"""

import argparse
import pandas as pd
import sys

def load_ground_truth(path):
    """
    Expects CSV with at least: 
        SampleID, TrueHPO_ID, TrueDO_ID
    or whatever columns you want to compare.
    """
    return pd.read_csv(path)

def measure_accuracy(merged_df, pred_col, true_col):
    """
    Returns (accuracy, n_eval) = fraction where predicted == true on non-null truth rows.
    """
    sub = merged_df.dropna(subset=[true_col])  # only evaluate rows that have ground truth
    if len(sub) == 0:
        return 0.0, 0
    matches = (sub[pred_col] == sub[true_col]).sum()
    n_eval = len(sub)
    return float(matches) / n_eval, n_eval

def main():
    parser = argparse.ArgumentParser(description="Compare Ontology Mapping to ground truth (optional).")
    parser.add_argument("--processed_csv", required=True, help="PhenoQC output CSV (post-processing).")
    parser.add_argument("--ground_truth", required=False, default=None,
                        help="CSV with columns [id_column, TrueHPO_ID, TrueDO_ID]. If not provided, skip accuracy check.")
    parser.add_argument("--id_column", default="SampleID", help="Name of the shared ID column.")
    parser.add_argument("--hpo_column", default="HPO_ID", help="Column in processed_csv with predicted HPO ID.")
    parser.add_argument("--do_column", default="DO_ID", help="Column in processed_csv with predicted DO ID.")
    args = parser.parse_args()

    # 1) Load the processed data
    try:
        df_proc = pd.read_csv(args.processed_csv)
    except Exception as ex:
        print(f"[ERROR] Could not read processed CSV: {ex}", file=sys.stderr)
        return

    print(f"[INFO] Loaded processed CSV: shape={df_proc.shape}")

    # 2) If no ground_truth file is provided, we skip accuracy checks
    if not args.ground_truth:
        print("[INFO] No ground truth provided; skipping ontology accuracy checks.")
        return

    # 3) Load ground truth
    try:
        df_gt = load_ground_truth(args.ground_truth)
        print(f"[INFO] Loaded ground truth: shape={df_gt.shape}")
    except Exception as ex:
        print(f"[ERROR] Could not read ground_truth CSV: {ex}", file=sys.stderr)
        return

    # 4) Merge on the ID column
    merged = pd.merge(df_proc, df_gt, how="left", on=args.id_column)

    # Check if the ground_truth has columns "TrueHPO_ID" or "TrueDO_ID"
    # If the user calls them something else, they'd adjust the code here
    needed_cols = ["TrueHPO_ID", "TrueDO_ID"]
    for col in needed_cols:
        if col not in merged.columns:
            print(f"[WARNING] Ground truth does not have '{col}' column. Accuracy for that ontology is skipped.")

    # 5) Evaluate HPO accuracy if we have TrueHPO_ID
    if "TrueHPO_ID" in merged.columns:
        hpo_acc, hpo_count = measure_accuracy(merged, args.hpo_column, "TrueHPO_ID")
        print(f"HPO Accuracy: {100.0 * hpo_acc:.2f}% on {hpo_count} records with known HPO ground truth.")
    else:
        print("[INFO] No TrueHPO_ID column in ground truth => skipping HPO accuracy.")

    # 6) Evaluate DO accuracy if we have TrueDO_ID
    if "TrueDO_ID" in merged.columns:
        do_acc, do_count = measure_accuracy(merged, args.do_column, "TrueDO_ID")
        print(f"DO Accuracy: {100.0 * do_acc:.2f}% on {do_count} records with known DO ground truth.")
    else:
        print("[INFO] No TrueDO_ID column in ground truth => skipping DO accuracy.")

    print("[DONE] Ontology accuracy check completed.")

if __name__ == "__main__":
    main()
