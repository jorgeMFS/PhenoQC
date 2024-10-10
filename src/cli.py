import argparse
import os
from .batch_processing import batch_process

def parse_arguments():
    """
    Parses command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="PhenoQC - Phenotypic Data Quality Control Toolkit")
    parser.add_argument('--input', nargs='+', required=True, help='Input phenotypic data files (CSV, TSV, JSON)')
    parser.add_argument('--output', required=True, help='Output directory for reports')
    parser.add_argument('--schema', required=True, help='Path to the JSON schema file')
    parser.add_argument('--mapping', required=True, help='Path to the HPO mapping JSON file')
    parser.add_argument('--custom-mapping', help='Path to the custom mapping JSON file', default=None)
    parser.add_argument('--impute', choices=['mean', 'median'], default='mean', help='Imputation strategy for missing data')
    return parser.parse_args()

def main():
    args = parse_arguments()
    
    # Ensure output directory exists
    os.makedirs(args.output, exist_ok=True)
    
    # Process files
    results = batch_process(
        files=args.input,
        file_type='csv' if all(f.endswith('.csv') for f in args.input) else 'tsv' if all(f.endswith('.tsv') for f in args.input) else 'json',
        schema_path=args.schema,
        hpo_terms_path=args.mapping,
        custom_mappings_path=args.custom_mapping,
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