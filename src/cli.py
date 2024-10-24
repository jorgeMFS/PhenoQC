import argparse
import os
from batch_processing import batch_process
from logging_module import setup_logging, log_activity

SUPPORTED_EXTENSIONS = {'.csv', '.tsv', '.json'}

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
    return parser.parse_args()

def collect_files(input_paths, recursive=False):
    """
    Collects all supported files from the input paths.

    Args:
        input_paths (list): List of file or directory paths.
        recursive (bool): Whether to scan directories recursively.

    Returns:
        list: List of file paths to process.
    """
    collected_files = []
    for path in input_paths:
        if os.path.isfile(path):
            ext = os.path.splitext(path)[1].lower()
            if ext in SUPPORTED_EXTENSIONS:
                collected_files.append(os.path.abspath(path))
            else:
                print(f"❌ Unsupported file type skipped: {path}")
        elif os.path.isdir(path):
            if recursive:
                for root, _, files in os.walk(path):
                    for file in files:
                        file_path = os.path.join(root, file)
                        ext = os.path.splitext(file_path)[1].lower()
                        if ext in SUPPORTED_EXTENSIONS:
                            collected_files.append(os.path.abspath(file_path))
            else:
                for file in os.listdir(path):
                    file_path = os.path.join(path, file)
                    if os.path.isfile(file_path):
                        ext = os.path.splitext(file_path)[1].lower()
                        if ext in SUPPORTED_EXTENSIONS:
                            collected_files.append(os.path.abspath(file_path))
        else:
            print(f"❌ Invalid path skipped: {path}")
    return collected_files

def main():
    # Setup logging
    setup_logging()

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
        target_ontologies=args.ontologies
    )

    # Summary of results
    for result in results:
        status = result['status']
        file = result['file']
        if status == 'Processed':
            log_activity(f"{os.path.basename(file)} processed successfully.", level='info')
        elif status == 'Invalid':
            log_activity(f"{os.path.basename(file)} failed validation: {result['error']}", level='warning')
        else:
            log_activity(f"{os.path.basename(file)} encountered an error: {result['error']}", level='error')

if __name__ == "__main__":
    main()