import argparse
import json
import os
import datetime

from phenoqc.batch_processing import batch_process
from phenoqc.logging_module import setup_logging, log_activity
from phenoqc.utils.zip_utils import extract_zip
from phenoqc.quality_metrics import QUALITY_METRIC_CHOICES

SUPPORTED_EXTENSIONS = {'.csv', '.tsv', '.json', '.zip'}

def parse_arguments():
    """
    Parses command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description='PhenoQC - Phenotypic Data Quality Control Toolkit')
    parser.add_argument('--input', nargs='+', required=True, help='Input phenotypic data files or directories (csv, tsv, json)')
    parser.add_argument('--output', default='./reports/', help='Output directory for reports')
    parser.add_argument('--schema', required=True, help='Path to the JSON schema file')
    parser.add_argument('--config', default='config.yaml', help='Path to the configuration YAML file')
    parser.add_argument('--custom_mappings', help='Path to custom mapping JSON file', default=None)
    parser.add_argument(
        '--impute',
        choices=['mean', 'median', 'mode', 'knn', 'mice', 'svd', 'none'],
        default='mean',
        help='Imputation strategy for missing data'
    )
    parser.add_argument(
        '--impute-params',
        type=json.loads,
        default=None,
        help='JSON object of parameters for the imputation strategy (e.g., {"n_neighbors": 5})'
    )
    parser.add_argument(
        '--impute-tuning',
        choices=['on', 'off'],
        default='off',
        help='Enable quick tuning for imputation (mask-and-score)'
    )
    # Optional stability diagnostics for imputation
    parser.add_argument(
        '--impute-diagnostics',
        choices=['on', 'off'],
        default='off',
        help='Enable imputation stability diagnostics (repeatability)'
    )
    parser.add_argument('--diag-repeats', type=int, default=5, help='Repeats for stability diagnostic (default 5)')
    parser.add_argument('--diag-mask-fraction', type=float, default=0.10, help='Mask fraction for stability diagnostic (default 0.10)')
    parser.add_argument('--diag-scoring', choices=['MAE','RMSE'], default='MAE', help='Scoring metric for stability diagnostic (default MAE)')
    parser.add_argument('--recursive', action='store_true', help='Enable recursive directory scanning for nested files')
    parser.add_argument('--unique_identifiers', nargs='+', required=True, help='List of column names that uniquely identify a record')
    parser.add_argument('--ontologies', nargs='+', help='List of ontologies to map to (e.g., HPO DO MPO)', default=None)
    parser.add_argument(
        '--phenotype_columns',
        type=lambda x: {x: ["HPO"]} if '{' not in x else json.loads(x),
        help='Either a single column name or a JSON mapping of columns to ontologies (e.g., \'{"PrimaryPhenotype": ["HPO"]}\')'
    )
    parser.add_argument(
        '--phenotype_column',
        help='[Deprecated] Use --phenotype_columns instead'
    )
    parser.add_argument(
        '--quality-metrics',
        nargs='+',
        choices=QUALITY_METRIC_CHOICES + ['all'],
        help='Additional quality metrics to evaluate',
        default=None
    )
    # Redundancy metric configuration
    parser.add_argument('--redundancy-threshold', type=float, default=None, help='Correlation threshold for redundancy (overrides config if set)')
    parser.add_argument('--redundancy-method', choices=['pearson','spearman'], default=None, help='Correlation method for redundancy (overrides config if set)')
    # Offline mode (cached/local ontologies only)
    parser.add_argument('--offline', action='store_true', help='Use cached/local ontologies only; do not download')
    # Optional thresholds for imputation-bias diagnostic
    parser.add_argument('--bias-smd-threshold', type=float, default=0.10, help='SMD threshold for bias warning (default 0.10)')
    parser.add_argument('--bias-var-low', type=float, default=0.5, help='Variance ratio lower bound (default 0.5)')
    parser.add_argument('--bias-var-high', type=float, default=2.0, help='Variance ratio upper bound (default 2.0)')
    parser.add_argument('--bias-ks-alpha', type=float, default=0.05, help='KS-test alpha (default 0.05)')
    parser.add_argument(
        '--label-column',
        help='Optional label column name for class distribution summary',
        default=None,
    )
    parser.add_argument(
        '--imbalance-threshold',
        type=float,
        default=0.10,
        help='Minority class proportion threshold to flag imbalance (default: 0.10)'
    )
    parser.add_argument(
        '--protected-columns', '--protected_columns',
        nargs='+',
        dest='protected_columns',
        help='Columns to protect from imputation and tuning (accepts space or comma separated)',
        default=[]
    )
    args = parser.parse_args()
    
    # Convert old phenotype_column to new format if specified
    if args.phenotype_column:
        if not args.phenotype_columns:  # Only use if phenotype_columns not specified
            args.phenotype_columns = {args.phenotype_column: ["HPO"]}
        args.phenotype_column = None  # Clear the old argument

    if args.quality_metrics and 'all' in args.quality_metrics:
        args.quality_metrics = QUALITY_METRIC_CHOICES

    # Normalize protected_columns: allow both space-separated and comma-separated entries
    if isinstance(args.protected_columns, list):
        normalized = []
        for entry in args.protected_columns:
            if isinstance(entry, str):
                normalized.extend([p.strip() for p in entry.split(',') if p.strip()])
        args.protected_columns = normalized
    elif isinstance(args.protected_columns, str):
        args.protected_columns = [p.strip() for p in args.protected_columns.split(',') if p.strip()]

    return args

def collect_files(input_paths, recursive=False):
    """
    Collects all supported files (.csv, .tsv, .json) from the input paths.
    If a ZIP is found, it is extracted to a temp dir, then we apply the same logic.
    If not recursive, we only do a top-level or a single subfolder pass.
    """
    collected_files = []
    print(f"[DEBUG] Starting collect_files with input_paths={input_paths}, recursive={recursive}")

    for path in input_paths:
        print(f"[DEBUG] Checking path: {path}")
        if os.path.isfile(path):
            ext = os.path.splitext(path)[1].lower()

            if ext == '.zip':
                log_activity(f"Detected ZIP file: {path}", level='info')
                extracted_dir, err = extract_zip(path)
                print(f"[DEBUG] Extracted_dir = {extracted_dir}, err={err}")
                if err:
                    print(f"❌ Failed to extract ZIP '{path}': {err}")
                    continue

                if recursive:
                    # full recursive
                    for root, dirs, files in os.walk(extracted_dir):
                        for file_name in files:
                            ext2 = os.path.splitext(file_name)[1].lower()
                            print(f"[DEBUG] Found extracted file: {os.path.join(root, file_name)} with ext={ext2}")
                            if ext2 in {'.csv', '.tsv', '.json'}:
                                collected_files.append(os.path.join(root, file_name))
                else:
                    # non-recursive: let's do top-level + 1 layer
                    for idx, (root, dirs, files) in enumerate(os.walk(extracted_dir)):
                        for file_name in files:
                            ext2 = os.path.splitext(file_name)[1].lower()
                            print(f"[DEBUG] Found extracted file: {os.path.join(root, file_name)} with ext={ext2}")
                            if ext2 in {'.csv', '.tsv', '.json'}:
                                collected_files.append(os.path.join(root, file_name))
                        # break after scanning the top-level (idx=0) + direct subdirectories (idx=1).
                        # If you want only the top-level, do if idx == 0: break
                        if idx >= 1:
                            break

            elif ext in {'.csv', '.tsv', '.json'}:
                collected_files.append(os.path.abspath(path))
            else:
                print(f"❌ Unsupported file type skipped: {path}")

        elif os.path.isdir(path):
            # if user passes a directory instead of a zip
            if recursive:
                for root, dirs, files in os.walk(path):
                    for file_name in files:
                        ext2 = os.path.splitext(file_name)[1].lower()
                        if ext2 in {'.csv', '.tsv', '.json'}:
                            collected_files.append(os.path.abspath(os.path.join(root, file_name)))
            else:
                # just top-level
                for file_name in os.listdir(path):
                    file_path = os.path.join(path, file_name)
                    if os.path.isfile(file_path):
                        ext2 = os.path.splitext(file_name)[1].lower()
                        if ext2 in {'.csv', '.tsv', '.json'}:
                            collected_files.append(os.path.abspath(file_path))
        else:
            print(f"❌ Invalid path skipped: {path}")

    print(f"[DEBUG] collect_files returning {collected_files}")
    return collected_files


def main():
    # Setup logging
    now_str = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    single_log_filename = f"phenoqc_{now_str}.log"
    setup_logging(log_file=single_log_filename, mode='w')

    args = parse_arguments()

    # Collect all supported files
    files_to_process = collect_files(args.input, recursive=args.recursive)

    if not files_to_process:
        log_activity("No valid input files found to process.", level='error')
        print("❌ No valid input files found to process.")
        return

    # Ensure output directory exists
    os.makedirs(args.output, exist_ok=True)

    # Log the start of batch processing
    log_activity(f"Starting batch processing with {len(files_to_process)} files.")

    # Process files
    results = batch_process(
        files=files_to_process,
        schema_path=args.schema,
        config_path=args.config,
        unique_identifiers=args.unique_identifiers,
        custom_mappings_path=args.custom_mappings,
        impute_strategy=args.impute,
        impute_params=args.impute_params,
        impute_tuning_enable=(args.impute_tuning == 'on'),
        output_dir=args.output,
        target_ontologies=args.ontologies,
        phenotype_columns=args.phenotype_columns,
        log_file_for_children=single_log_filename,
        quality_metrics=args.quality_metrics,
        class_label_column=args.label_column,
        imbalance_threshold=args.imbalance_threshold,
        bias_smd_threshold=args.bias_smd_threshold,
        bias_var_low=args.bias_var_low,
        bias_var_high=args.bias_var_high,
        bias_ks_alpha=args.bias_ks_alpha,
        protected_columns=args.protected_columns,
        impute_diag_enable=(args.impute_diagnostics == 'on'),
        diag_repeats=args.diag_repeats,
        diag_mask_fraction=args.diag_mask_fraction,
        diag_scoring=args.diag_scoring,
        redundancy_threshold=args.redundancy_threshold,
        redundancy_method=args.redundancy_method,
        offline=bool(args.offline),
    )
    
    for result in results:
        status = result.get('status')
        file_path = result.get('file')
        err_msg = result.get('error', '')

        base_name = os.path.basename(file_path) if file_path else "<Unknown>"

        if status == 'Processed':
            log_activity(f"{base_name} processed successfully.", level='info')
        elif status == 'ProcessedWithWarnings':
            log_activity(
                f"{base_name} completed with warnings. {err_msg}",
                level='warning'
            )
        elif status == 'Invalid':
            log_activity(
                f"{base_name} failed validation: {err_msg}",
                level='warning'
            )
        elif status == 'Error':
            log_activity(
                f"{base_name} encountered an error: {err_msg}",
                level='error'
            )
        else:
            # fallback
            log_activity(
                f"{base_name} finished with unrecognized status '{status}': {err_msg}",
                level='warning'
            )

    print(f"✅ Finished processing {len(results)} files.")
if __name__ == "__main__":
    main()
