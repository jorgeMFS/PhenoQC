import concurrent.futures
import os
import json
import pandas as pd
from input import load_data
from validation import DataValidator
from mapping import OntologyMapper
from missing_data import detect_missing_data, impute_missing_data, flag_missing_data_records
from reporting import generate_qc_report, create_visual_summary
from logging_module import log_activity
from configuration import load_config
from tqdm import tqdm

def get_file_type(file_path):
    _, ext = os.path.splitext(file_path.lower())
    if ext == '.csv':
        return 'csv'
    elif ext == '.tsv':
        return 'tsv'
    elif ext == '.json':
        return 'json'
    else:
        raise ValueError(f"Unsupported file extension: {ext}")

def process_file(
    file_path,
    schema,
    ontology_mapper,
    unique_identifiers,
    custom_mappings=None,
    impute_strategy='mean',
    field_strategies=None,
    output_dir='reports',
    target_ontologies=None
):
    file_type = get_file_type(file_path)
    log_activity(f"Processing file: {file_path}")
    print(f"Processing file: {file_path}")

    try:
        # Initialize progress bar for file processing
        with tqdm(total=100, desc=f"Processing {os.path.basename(file_path)}") as pbar:
            data = load_data(file_path, file_type)
            pbar.update(10)
            log_activity("Data loaded successfully.")

            df = pd.DataFrame(data) if isinstance(data, list) else data

            # Initialize DataValidator
            validator = DataValidator(df, schema, unique_identifiers)

            # Run all validations
            validation_results = validator.run_all_validations()
            pbar.update(20)

            # Check Format Validation
            if not validation_results["Format Validation"]:
                error_msg = "Format validation failed. Schema compliance issues detected."
                log_activity(f"{file_path}: {error_msg}", level='error')
                pbar.close()
                return {'file': file_path, 'status': 'Invalid', 'error': error_msg}

            # Missing Data Handling
            missing = detect_missing_data(df)
            pbar.update(10)
            if not missing.empty:
                # Flag records with missing data
                df = flag_missing_data_records(df)
                flagged_records_count = df['MissingDataFlag'].sum()
                log_activity(f"{file_path}: {flagged_records_count} records flagged for missing data.")

                # Save flagged records for manual review
                flagged_records = df[df['MissingDataFlag']]
                flagged_records_path = os.path.join(
                    output_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}_flagged_records.csv"
                )
                flagged_records.to_csv(flagged_records_path, index=False)
                log_activity(f"Flagged records saved to {flagged_records_path}.")

                # Impute missing data
                df = impute_missing_data(df, strategy=impute_strategy, field_strategies=field_strategies)
                log_activity(
                    f"{file_path}: Missing data imputed using strategy '{impute_strategy}' "
                    f"with field-specific strategies: {field_strategies}"
                )

                # Recalculate MissingDataFlag after imputation
                df = flag_missing_data_records(df)
                flagged_records_count = df['MissingDataFlag'].sum()
                log_activity(f"{file_path}: {flagged_records_count} records have missing data after imputation.")
                pbar.update(20)
            else:
                flagged_records_count = 0
                pbar.update(20)

            # Check for Duplicate Records
            duplicates = validation_results["Duplicate Records"]
            if not duplicates.empty:
                log_activity(f"{file_path}: Duplicate records found.", level='warning')
                # Optionally, handle duplicates as needed

            pbar.update(10)

            # Ontology Mapping
            if 'Phenotype' in df.columns:
                phenotypic_terms = df['Phenotype'].unique().tolist()
                mappings = ontology_mapper.map_terms(phenotypic_terms, target_ontologies, custom_mappings)

                # Add mapped IDs to the DataFrame
                for ontology_id in target_ontologies or [ontology_mapper.default_ontology]:
                    mapped_column = f"{ontology_id}_ID"
                    df[mapped_column] = df['Phenotype'].apply(lambda x: mappings.get(x, {}).get(ontology_id))

                pbar.update(20)
            else:
                log_activity(f"{file_path}: 'Phenotype' column not found.", level='error')
                pbar.close()
                return {'file': file_path, 'status': 'Invalid', 'error': "'Phenotype' column not found."}

            # Generate Reports
            report_path = os.path.join(
                output_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}_report.pdf"
            )
            generate_qc_report(validation_results, missing, flagged_records_count, report_path)
            create_visual_summary(
                df,
                output_image_path=os.path.join(
                    output_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}_visual_summary.html"
                )
            )
            log_activity(f"{file_path}: QC report generated at {report_path}.")
            pbar.update(10)

            # Save the cleaned DataFrame
            output_data_file = os.path.join(output_dir, os.path.basename(file_path))
            df.to_csv(output_data_file, index=False)
            log_activity(f"{file_path}: Processed data saved at {output_data_file}")

            pbar.update(10)
            pbar.close()

            # After processing, collect necessary data
            result = {
                'file': file_path,
                'status': 'Processed',
                'error': None,
                'validation_results': validation_results,
                'missing_data': missing,
                'flagged_records_count': flagged_records_count
            }

            return result

    except Exception as e:
        log_activity(f"Error processing file {file_path}: {str(e)}", level='error')
        return {'file': file_path, 'status': 'Error', 'error': str(e)}

def collect_files(inputs, recursive=True):
    """
    Collects all supported files from the provided input paths.

    Args:
        inputs (list): List of file or directory paths.
        recursive (bool): Whether to scan directories recursively.

    Returns:
        list: List of file paths.
    """
    supported_extensions = {'.csv', '.tsv', '.json'}
    collected_files = []

    for input_path in inputs:
        if os.path.isfile(input_path):
            ext = os.path.splitext(input_path)[1].lower()
            if ext in supported_extensions:
                collected_files.append(input_path)
        elif os.path.isdir(input_path):
            for root, dirs, files in os.walk(input_path):
                for file in files:
                    ext = os.path.splitext(file)[1].lower()
                    if ext in supported_extensions:
                        collected_files.append(os.path.join(root, file))
                if not recursive:
                    break
        else:
            log_activity(f"Input path '{input_path}' is neither a file nor a directory.", level='warning')

    return collected_files

def batch_process(
    files,
    schema_path,
    config_path,
    unique_identifiers,
    custom_mappings_path=None,
    impute_strategy='mean',
    output_dir='reports',
    target_ontologies=None
):
    # Load schema
    with open(schema_path, 'r') as f:
        schema = json.load(f)

    # Load configuration
    config = load_config(config_path)

    # Extract field-specific imputation strategies from config
    field_strategies = config.get('imputation_strategies', {})

    # Initialize OntologyMapper
    ontology_mapper = OntologyMapper(config_path=config_path)

    # Load custom mappings if provided
    custom_mappings = None
    if custom_mappings_path and os.path.exists(custom_mappings_path):
        with open(custom_mappings_path, 'r') as f:
            custom_mappings = json.load(f)

    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)

    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = {
            executor.submit(
                process_file,
                file_path,
                schema,
                ontology_mapper,
                unique_identifiers,
                custom_mappings,
                impute_strategy,
                field_strategies,
                output_dir,
                target_ontologies
            ): file_path for file_path in files
        }
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Batch Processing"):
            file_path = futures[future]
            try:
                result = future.result()
                results.append(result)
                if result['status'] == 'Processed':
                    print(f"✅ {os.path.basename(result['file'])} processed successfully.")
                elif result['status'] == 'Invalid':
                    print(f"⚠️ {os.path.basename(result['file'])} failed validation: {result['error']}")
                else:
                    print(f"❌ {os.path.basename(result['file'])} encountered an error: {result['error']}")
            except Exception as e:
                log_activity(f"Error in processing {file_path}: {str(e)}", level='error')
                print(f"❌ Error in processing {os.path.basename(file_path)}: {str(e)}")

    return results
