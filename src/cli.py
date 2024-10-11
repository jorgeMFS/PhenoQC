import argparse
import os
from batch_processing import batch_process

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
    parser.add_argument('--mapping', required=True, help='Path to the HPO mapping JSON file')
    parser.add_argument('--custom_mappings', help='Path to custom mapping JSON file', default=None)
    parser.add_argument('--impute', choices=['mean', 'median'], default='mean', help='Imputation strategy for missing data')
    parser.add_argument('--recursive', action='store_true', help='Enable recursive directory scanning for nested files')
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
    args = parse_arguments()

    # Collect all supported files
    files_to_process = collect_files(args.input, recursive=args.recursive)

    if not files_to_process:
        print("❌ No valid input files found to process.")
        return

    # Ensure output directory exists
    os.makedirs(args.output, exist_ok=True)

    # Process files
    results = batch_process(
        files=files_to_process,
        schema_path=args.schema,
        hpo_terms_path=args.mapping,
        custom_mappings_path=args.custom_mappings,
        impute_strategy=args.impute
    )

    # Summary of results
    for result in results:
        status = result['status']
        file = result['file']
        if status == 'Processed':
            print(f"✅ {file} processed successfully.")
        elif status == 'Invalid':
            print(f"⚠️ {file} failed validation: {result['error']}")
        else:
            print(f"❌ {file} encountered an error: {result['error']}")

if __name__ == '__main__':
    main()