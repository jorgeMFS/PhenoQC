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

            # Prepare output file path
            output_data_file = os.path.join(output_dir, os.path.basename(file_path))
            # Remove existing output file if exists
            if os.path.exists(output_data_file):
                os.remove(output_data_file)

            # Initialize a flag to write header only once
            write_header = True

            # Initialize mapping success rates accumulators
            cumulative_mapping_stats = {}
            target_ontologies = target_ontologies or ontology_mapper.default_ontologies
            for ontology_id in target_ontologies:
                cumulative_mapping_stats[ontology_id] = {'total_terms': 0, 'mapped_terms': 0}

            # Initialize sample DataFrame for visualizations
            sample_df = pd.DataFrame()
            sample_size_per_chunk = 1000  # Adjust as needed
            max_total_samples = 10000     # Maximum total samples for visualization

            # Process chunks
            chunk_progress = 80  # Percentage of progress allocated to chunk processing
            chunks_processed = 0
            for chunk in data_iterator:
                chunks_processed += 1
                total_records += len(chunk)

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
                # Note: After imputation, MissingDataFlag should decrease or remain the same

                # Collect phenotype terms
                if 'Phenotype' in chunk.columns:
                    phenotype_terms_set.update(chunk['Phenotype'].unique())
                else:
                    log_activity(f"{file_path}: 'Phenotype' column not found in chunk.", level='error')
                    pbar.close()
                    return {'file': file_path, 'status': 'Invalid', 'error': "'Phenotype' column not found in chunk."}

                # Ontology Mapping
                if 'Phenotype' in chunk.columns:
                    phenotypic_terms = chunk['Phenotype'].unique()
                    mappings = ontology_mapper.map_terms(phenotypic_terms, target_ontologies, custom_mappings)

                    # Add mapped IDs to the DataFrame
                    for ontology_id in target_ontologies:
                        mapped_column = f"{ontology_id}_ID"
                        chunk[mapped_column] = chunk['Phenotype'].apply(
                            lambda x: mappings.get(x, {}).get(ontology_id)
                        )

                        # Update mapping success rates
                        mapped_terms = chunk[mapped_column].notnull().sum()
                        cumulative_mapping_stats[ontology_id]['total_terms'] += len(chunk)
                        cumulative_mapping_stats[ontology_id]['mapped_terms'] += mapped_terms
                else:
                    log_activity(f"{file_path}: 'Phenotype' column not found in chunk.", level='error')
                    pbar.close()
                    return {'file': file_path, 'status': 'Invalid', 'error': "'Phenotype' column not found in chunk."}

                # Collect sample data for visualizations
                if len(sample_df) < max_total_samples:
                    remaining_samples = max_total_samples - len(sample_df)
                    sample_size = min(sample_size_per_chunk, remaining_samples)
                    if len(chunk) > sample_size:
                        sample_chunk = chunk.sample(n=sample_size, random_state=42)
                    else:
                        sample_chunk = chunk.copy()
                    sample_df = pd.concat([sample_df, sample_chunk], ignore_index=True)

                # Write processed chunk to the output file
                chunk.to_csv(output_data_file, mode='a', index=False, header=write_header)
                if write_header:
                    write_header = False

                # Update progress bar
                pbar.update(chunk_progress / max(1, total_records / chunksize))

            pbar.update(5)  # Update remaining progress

            if not validation_results["Format Validation"]:
                error_msg = "Format validation failed. Schema compliance issues detected."
                log_activity(f"{file_path}: {error_msg}", level='error')
                pbar.close()
                return {'file': file_path, 'status': 'Invalid', 'error': error_msg}

            # Calculate mapping success rates
            mapping_success_rates = {}
            for ontology_id, stats in cumulative_mapping_stats.items():
                total_terms = stats['total_terms']
                mapped_terms = stats['mapped_terms']
                success_rate = (mapped_terms / total_terms) * 100 if total_terms > 0 else 0
                mapping_success_rates[ontology_id] = {
                    'total_terms': total_terms,
                    'mapped_terms': mapped_terms,
                    'success_rate': success_rate
                }

            # Generate Reports
            report_path = os.path.join(
                output_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}_report.{report_format}"
            )

            # Create visualizations using sample_df
            figs = create_visual_summary(sample_df, output_image_path=None)
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
                impute_strategy,
                report_path,
                report_format
            )
            log_activity(f"{file_path}: QC report generated at {report_path}.")
            pbar.update(5)

            log_activity(f"{file_path}: Processed data saved at {output_data_file}")

            pbar.update(5)
            pbar.close()

            # After processing, collect necessary data
            # result = {
            #     'file': file_path,
            #     'status': 'Processed',
            #     'error': None,
            #     'validation_results': validation_results,
            #     'missing_data': missing_counts,
            #     'flagged_records_count': flagged_records_count
            # }

            # return result
            result = {
                'file': file_path,
                'status': 'Processed',
                'error': None,
                'validation_results': validation_results,
                'missing_data': missing_counts,
                'flagged_records_count': flagged_records_count,
                'processed_file_path': output_data_file,  # Add this line
                'mapping_success_rates': mapping_success_rates,  # Include if needed for reporting
                'visualization_images': visualization_images   # Include if needed for reporting
            }

            return result

    except Exception as e:
        log_activity(f"Error processing file {file_path}: {str(e)}", level='error')
        return {'file': file_path, 'status': 'Error', 'error': str(e)}

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
