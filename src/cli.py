import argparse
import json
import os
import sys
import datetime

# Support running both as a module (with package context) and as a script.
if __package__:
    from .batch_processing import batch_process
    from .logging_module import setup_logging, log_activity
    from .utils.zip_utils import extract_zip
else:  # pragma: no cover - allows execution without installation
    from importlib import import_module
    import types
    current_dir = os.path.dirname(os.path.abspath(__file__))
    if current_dir not in sys.path:
        sys.path.insert(0, current_dir)
    if "phenoqc" not in sys.modules:
        pkg = types.ModuleType("phenoqc")
        pkg.__path__ = [current_dir]
        sys.modules["phenoqc"] = pkg
    batch_process = import_module("phenoqc.batch_processing").batch_process
    logging_mod = import_module("phenoqc.logging_module")
    setup_logging = logging_mod.setup_logging
    log_activity = logging_mod.log_activity
    extract_zip = import_module("phenoqc.utils.zip_utils").extract_zip

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
        choices=['accuracy', 'redundancy', 'traceability', 'timeliness', 'all'],
        help='Additional quality metrics to evaluate',
        default=None
    )
    args = parser.parse_args()
    
    # Convert old phenotype_column to new format if specified
    if args.phenotype_column:
        if not args.phenotype_columns:  # Only use if phenotype_columns not specified
            args.phenotype_columns = {args.phenotype_column: ["HPO"]}
        args.phenotype_column = None  # Clear the old argument

    if args.quality_metrics and 'all' in args.quality_metrics:
        args.quality_metrics = ['accuracy', 'redundancy', 'traceability', 'timeliness']

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
        output_dir=args.output,
        target_ontologies=args.ontologies,
        phenotype_columns=args.phenotype_columns,
        log_file_for_children=single_log_filename,
        quality_metrics=args.quality_metrics
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