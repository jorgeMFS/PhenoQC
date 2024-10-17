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
    target_ontologies=None,
    report_format='pdf',
    chunksize=10000
):
    file_type = get_file_type(file_path)
    log_activity(f"Processing file: {file_path}")
    print(f"Processing file: {file_path}")

    try:
        # Initialize progress bar for file processing
        with tqdm(total=100, desc=f"Processing {os.path.basename(file_path)}") as pbar:
            data_iterator = load_data(file_path, file_type, chunksize=chunksize)
            pbar.update(5)
            log_activity("Data loading initiated.")

            # Initialize accumulators for global statistics
            validation_results = {
                "Format Validation": True,
                "Duplicate Records": pd.DataFrame(),
                "Conflicting Records": pd.DataFrame(),
                "Integrity Issues": pd.DataFrame(),
                "Referential Integrity Issues": pd.DataFrame()
            }
            missing_counts = pd.Series(dtype=int)
            total_records = 0
            flagged_records_count = 0
            phenotype_terms_set = set()
            df_list = []  # To collect processed chunks

            # Process chunks
            chunk_progress = 80  # Percentage of progress allocated to chunk processing
            chunk_number = 0
            for chunk in data_iterator:
                chunk_number += 1
                chunk_total = len(chunk)
                total_records += chunk_total

                # Initialize DataValidator for the chunk
                validator = DataValidator(chunk, schema, unique_identifiers)

                # Run format validation on the chunk
                format_valid = validator.validate_format()
                if not format_valid:
                    validation_results["Format Validation"] = False
                    validation_results["Integrity Issues"] = pd.concat(
                        [validation_results["Integrity Issues"], validator.integrity_issues]
                    )

                # Identify duplicates in the chunk
                duplicates = validator.identify_duplicates()
                if not duplicates.empty:
                    validation_results["Duplicate Records"] = pd.concat(
                        [validation_results["Duplicate Records"], duplicates]
                    )

                # Detect conflicts in the chunk
                conflicts = validator.detect_conflicts()
                if not conflicts.empty:
                    validation_results["Conflicting Records"] = pd.concat(
                        [validation_results["Conflicting Records"], conflicts]
                    )

                # Verify integrity in the chunk
                integrity_issues = validator.verify_integrity()
                if not integrity_issues.empty:
                    validation_results["Integrity Issues"] = pd.concat(
                        [validation_results["Integrity Issues"], integrity_issues]
                    )

                # Missing Data Handling
                missing = detect_missing_data(chunk)
                missing_counts = missing_counts.add(missing, fill_value=0)

                # Flag records with missing data
                chunk = flag_missing_data_records(chunk)
                chunk_flagged_count = chunk['MissingDataFlag'].sum()
                flagged_records_count += chunk_flagged_count

                # Impute missing data
                chunk = impute_missing_data(chunk, strategy=impute_strategy, field_strategies=field_strategies)

                # Recalculate MissingDataFlag after imputation
                chunk = flag_missing_data_records(chunk)
                chunk_flagged_count_after = chunk['MissingDataFlag'].sum()

                # Collect phenotype terms
                if 'Phenotype' in chunk.columns:
                    phenotype_terms_set.update(chunk['Phenotype'].unique())

                # Store processed chunk
                df_list.append(chunk)

                # Update progress
                pbar.update(chunk_progress / max(1, total_records / chunksize))

            pbar.update(5)  # Update remaining progress

            if not validation_results["Format Validation"]:
                error_msg = "Format validation failed. Schema compliance issues detected."
                log_activity(f"{file_path}: {error_msg}", level='error')
                pbar.close()
                return {'file': file_path, 'status': 'Invalid', 'error': error_msg}

            # Combine processed chunks into a single DataFrame
            df = pd.concat(df_list, ignore_index=True)

            # Ontology Mapping
            if 'Phenotype' in df.columns:
                phenotypic_terms = list(phenotype_terms_set)
                mappings = ontology_mapper.map_terms(phenotypic_terms, target_ontologies, custom_mappings)

                # Add mapped IDs to the DataFrame
                for ontology_id in target_ontologies or [ontology_mapper.default_ontology]:
                    mapped_column = f"{ontology_id}_ID"
                    df[mapped_column] = df['Phenotype'].apply(lambda x: mappings.get(x, {}).get(ontology_id))

                # Compute mapping success rates
                mapping_success_rates = {}
                for ontology_id in target_ontologies or [ontology_mapper.default_ontology]:
                    mapped_column = f"{ontology_id}_ID"
                    total_terms = len(df)
                    mapped_terms = df[mapped_column].notnull().sum()
                    success_rate = (mapped_terms / total_terms) * 100 if total_terms > 0 else 0
                    mapping_success_rates[ontology_id] = {
                        'total_terms': total_terms,
                        'mapped_terms': mapped_terms,
                        'success_rate': success_rate
                    }

                pbar.update(5)
            else:
                log_activity(f"{file_path}: 'Phenotype' column not found.", level='error')
                pbar.close()
                return {'file': file_path, 'status': 'Invalid', 'error': "'Phenotype' column not found."}

            # Generate Reports
            report_path = os.path.join(
                output_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}_report.{report_format}"
            )
            figs = create_visual_summary(df, output_image_path=None)
            # Save visualizations as images
            visualization_images = []
            for idx, fig in enumerate(figs):
                image_filename = f"{os.path.splitext(os.path.basename(file_path))[0]}_visual_{idx}.png"
                image_path = os.path.join(output_dir, image_filename)
                fig.write_image(image_path)
                visualization_images.append(image_path)
            generate_qc_report(
                validation_results,
                missing_counts,
                flagged_records_count,
                mapping_success_rates,
                visualization_images,
                report_path,
                report_format
            )
            log_activity(f"{file_path}: QC report generated at {report_path}.")
            pbar.update(5)

            # Save the cleaned DataFrame
            output_data_file = os.path.join(output_dir, os.path.basename(file_path))
            df.to_csv(output_data_file, index=False)
            log_activity(f"{file_path}: Processed data saved at {output_data_file}")

            pbar.update(5)
            pbar.close()

            # After processing, collect necessary data
            result = {
                'file': file_path,
                'status': 'Processed',
                'error': None,
                'validation_results': validation_results,
                'missing_data': missing_counts,
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
    target_ontologies=None,
    report_format='pdf',
    chunksize=10000
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
                target_ontologies,
                report_format,
                chunksize
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
