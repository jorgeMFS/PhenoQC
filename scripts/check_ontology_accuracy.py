#!/usr/bin/env python3

"""
check_ontology_accuracy.py

Compare PhenoQC's ontology-mapped columns (e.g. HPO_ID, DO_ID, MPO_ID)
against a ground truth CSV that has columns like:
    [SampleID, TrueHPO_ID, TrueDO_ID, TrueMPO_ID, TruePrimaryHPO_ID, etc.]

Example usage:

  # 1) Single HPO column
  python check_ontology_accuracy.py \
    --processed_csv ./reports/synthetic_hpo_data.csv \
    --ground_truth  ./output/synthetic_hpo_ground_truth.csv \
    --id_column SampleID \
    --hpo_column HPO_ID

  # 2) Multi-ontology
  python check_ontology_accuracy.py \
    --processed_csv ./output/reports/synthetic_multi_data.csv \
    --ground_truth  ./output/synthetic_multi_ground_truth.csv \
    --id_column SampleID \
    --hpo_column HPO_ID \
    --do_column DO_ID \
    --mpo_column MPO_ID \
    --secondary_hpo_column HPO_ID_2   # if you had a second HPO column

This script merges the processed CSV with the ground truth on `SampleID`
and calculates the fraction of rows for which predicted == ground_truth
(only among rows that actually have ground-truth data, i.e. non-null
"TrueXXX_ID" in the ground-truth CSV).
"""

import argparse
import pandas as pd
import sys

def load_ground_truth(path: str) -> pd.DataFrame:
    """
    Reads a CSV with columns like [SampleID, TrueHPO_ID, TrueDO_ID, TrueMPO_ID, etc.].
    """
    return pd.read_csv(path)

def measure_accuracy(merged_df: pd.DataFrame, pred_col: str, true_col: str) -> (float, int):
    """
    Returns (accuracy, n_eval), where accuracy = fraction (predicted == true)
    among rows with a non-null ground truth in `true_col`.

    merged_df: after merging processed CSV + ground truth
    pred_col:  name of predicted column in processed CSV
    true_col:  name of ground-truth column in ground_truth CSV
    """
    sub = merged_df.dropna(subset=[true_col])  # only evaluate where ground-truth is known
    if len(sub) == 0:
        return 0.0, 0
    # Compare exact string match
    matches = (sub[pred_col] == sub[true_col]).sum()
    n_eval = len(sub)
    return float(matches) / n_eval, n_eval

def main():
    parser = argparse.ArgumentParser(description="Compare Ontology Mapping to ground truth (optional).")

    parser.add_argument("--processed_csv", required=True,
                        help="PhenoQC output CSV (post-processing).")
    parser.add_argument("--ground_truth", required=False, default=None,
                        help="CSV with columns [SampleID, TrueHPO_ID, TrueDO_ID, etc.]. If not provided, skip checks.")
    parser.add_argument("--id_column", default="SampleID",
                        help="Name of the shared ID column in both CSVs.")

    # Potential ontology columns. You can add or remove as needed.
    parser.add_argument("--hpo_column", default=None,
                        help="Name of predicted HPO column in processed CSV (e.g. 'HPO_ID').")
    parser.add_argument("--secondary_hpo_column", default=None,
                        help="Name of a second predicted HPO column (e.g. 'HPO_ID2').")
    parser.add_argument("--do_column", default=None,
                        help="Name of predicted DO column in processed CSV (e.g. 'DO_ID').")
    parser.add_argument("--mpo_column", default=None,
                        help="Name of predicted MPO column in processed CSV (e.g. 'MPO_ID').")

    args = parser.parse_args()

    # 1) Load the processed CSV
    try:
        df_proc = pd.read_csv(args.processed_csv)
    except Exception as ex:
        print(f"[ERROR] Could not read processed CSV: {ex}", file=sys.stderr)
        return
    print(f"[INFO] Loaded processed CSV: shape={df_proc.shape}")

    # 2) If no ground_truth given, skip checks
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

    # 4) Merge on ID
    merged = pd.merge(df_proc, df_gt, how="left", on=args.id_column)
    print(f"[INFO] Merged shape={merged.shape} on '{args.id_column}'.")

    # 5) For each ontology column that the user specified, attempt to measure accuracy
    #    if the ground_truth CSV has the matching "TrueXYZ_ID" column.

    # We'll define a small helper function for repeated logic.
    def check_ontology_accuracy(predicted_col: str, ground_truth_col: str):
        """
        If predicted_col is not None and ground_truth_col in merged, measure accuracy.
        ground_truth_col might be "TrueHPO_ID", "TrueDO_ID", etc.
        """
        if predicted_col and predicted_col in merged.columns and ground_truth_col in merged.columns:
            acc, count = measure_accuracy(merged, predicted_col, ground_truth_col)
            print(f"Accuracy for {predicted_col} vs. {ground_truth_col}: "
                  f"{100.0 * acc:.2f}% (on {count} records with GT).")
        else:
            # either predicted_col not provided, or ground_truth_col is missing
            pass

    # 6) We map:
    #  - hpo_column -> compare with "TrueHPO_ID" (or "TruePrimaryHPO_ID")
    #  - secondary_hpo_column -> compare with "TrueSecondaryHPO_ID"
    #  - do_column -> compare with "TrueDO_ID"
    #  - mpo_column -> compare with "TrueMPO_ID"

    # Some people might store them as "TruePrimaryHPO_ID", "TrueSecondaryHPO_ID", "TrueDiseaseDO_ID", etc.
    # We'll attempt a few possible ground-truth column names for each.
    # If found, we measure. If not, skip.

    # Patterns for ground-truth columns:
    possible_hpo_cols = ["TrueHPO_ID", "TruePrimaryHPO_ID"]
    possible_secondary_cols = ["TrueSecondaryHPO_ID"]
    possible_do_cols = ["TrueDO_ID", "TrueDiseaseDO_ID"]  # matches ground truth
    possible_mpo_cols = ["TrueMPO_ID", "TrueTertiaryMPO_ID"]  # matches ground truth

    # 6a) HPO
    if args.hpo_column:
        # find a ground-truth column that exists among possible_hpo_cols
        found_hpo_gt = next((col for col in possible_hpo_cols if col in merged.columns), None)
        if found_hpo_gt:
            check_ontology_accuracy(args.hpo_column, found_hpo_gt)
        else:
            print(f"[INFO] No HPO ground-truth column found among {possible_hpo_cols}. Skipping HPO accuracy.")

    # 6b) Secondary HPO
    if args.secondary_hpo_column:
        found_hpo2_gt = next((col for col in possible_secondary_cols if col in merged.columns), None)
        if found_hpo2_gt:
            check_ontology_accuracy(args.secondary_hpo_column, found_hpo2_gt)
        else:
            print(f"[INFO] No secondary-HPO ground-truth column found among {possible_secondary_cols}. Skipping secondary HPO accuracy.")

    # 6c) DO
    if args.do_column:
        found_do_gt = next((col for col in possible_do_cols if col in merged.columns), None)
        if found_do_gt:
            check_ontology_accuracy(args.do_column, found_do_gt)
        else:
            print(f"[INFO] No DO ground-truth column found among {possible_do_cols}. Skipping DO accuracy.")

    # 6d) MPO
    if args.mpo_column:
        found_mpo_gt = next((col for col in possible_mpo_cols if col in merged.columns), None)
        if found_mpo_gt:
            check_ontology_accuracy(args.mpo_column, found_mpo_gt)
        else:
            print(f"[INFO] No MPO ground-truth column found among {possible_mpo_cols}. Skipping MPO accuracy.")

    # Add debug prints
    if args.do_column:
        print(f"\nDO columns in merged data: {[col for col in merged.columns if 'DO' in col]}")
        print(f"DO ground truth values (first 5):\n{merged[found_do_gt].head() if found_do_gt else 'No DO ground truth column found'}")
        print(f"DO mapped values (first 5):\n{merged[args.do_column].head() if args.do_column in merged.columns else 'No DO mapped column found'}")

    if args.mpo_column:
        print(f"\nMPO columns in merged data: {[col for col in merged.columns if 'MPO' in col or 'MP:' in col]}")
        print(f"MPO ground truth values (first 5):\n{merged[found_mpo_gt].head() if found_mpo_gt else 'No MPO ground truth column found'}")
        print(f"MPO mapped values (first 5):\n{merged[args.mpo_column].head() if args.mpo_column in merged.columns else 'No MPO mapped column found'}")

    print("[DONE] Ontology accuracy check completed.")

if __name__ == "__main__":
    main()
