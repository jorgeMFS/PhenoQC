# batch_processing.py

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
    chunksize=10000,
    phenotype_column='Phenotype'  # New parameter with default value
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

            # Initialize accumulators for validation results
            format_valid = True
            duplicate_records = []
            conflicting_records = []
            integrity_issues = []
            missing_counts = pd.Series(dtype=int)
            anomalies_detected = pd.DataFrame()

            # Accumulator for unique identifiers to check duplicates across chunks
            unique_id_set = set()

            # Process chunks
            chunk_progress = 80  # Percentage of progress allocated to chunk processing
            total_chunks = 0
            for chunk in data_iterator:
                total_chunks += 1
                total_records += len(chunk)

                # Initialize DataValidator for the chunk
                validator = DataValidator(chunk, schema, unique_identifiers)

                # Run format validation on the chunk
                chunk_format_valid = validator.validate_format()
                if not chunk_format_valid:
                    format_valid = False
                    integrity_issues.append(validator.integrity_issues)

                # Identify duplicates in the chunk
                chunk_duplicates = validator.identify_duplicates()
                if not chunk_duplicates.empty:
                    duplicate_records.append(chunk_duplicates)

                # Detect conflicts in the chunk
                chunk_conflicts = validator.detect_conflicts()
                if not chunk_conflicts.empty:
                    conflicting_records.append(chunk_conflicts)

                # Verify integrity in the chunk
                chunk_integrity_issues = validator.verify_integrity()
                if not chunk_integrity_issues.empty:
                    integrity_issues.append(chunk_integrity_issues)

                # Detect anomalies in the chunk
                validator.detect_anomalies()
                if not validator.anomalies.empty:
                    anomalies_detected = pd.concat([anomalies_detected, validator.anomalies])

                # Update unique identifier set for global duplicate detection
                ids_in_chunk = set(map(tuple, chunk[unique_identifiers].drop_duplicates().values.tolist()))
                duplicates_in_ids = unique_id_set.intersection(ids_in_chunk)
                if duplicates_in_ids:
                    # Records with duplicate unique identifiers across chunks
                    duplicate_records.append(chunk[chunk[unique_identifiers].apply(tuple, axis=1).isin(duplicates_in_ids)])
                unique_id_set.update(ids_in_chunk)

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
                if phenotype_column in chunk.columns:
                    phenotype_terms_set.update(chunk[phenotype_column].unique())
                else:
                    log_activity(f"{file_path}: '{phenotype_column}' column not found in chunk.", level='error')
                    pbar.close()
                    return {'file': file_path, 'status': 'Invalid', 'error': f"'{phenotype_column}' column not found in chunk."}

                # Ontology Mapping
                phenotypic_terms = chunk[phenotype_column].unique()
                mappings = ontology_mapper.map_terms(phenotypic_terms, target_ontologies, custom_mappings)

                # Add mapped IDs to the DataFrame
                for ontology_id in target_ontologies:
                    mapped_column = f"{ontology_id}_ID"
                    chunk[mapped_column] = chunk[phenotype_column].apply(
                        lambda x: mappings.get(x, {}).get(ontology_id)
                    )

                    # Update mapping success rates
                    mapped_terms = chunk[mapped_column].notnull().sum()
                    cumulative_mapping_stats[ontology_id]['total_terms'] += len(chunk)
                    cumulative_mapping_stats[ontology_id]['mapped_terms'] += mapped_terms

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

            if not format_valid:
                error_msg = "Format validation failed. Schema compliance issues detected."
                log_activity(f"{file_path}: {error_msg}", level='error')
                pbar.close()
                return {'file': file_path, 'status': 'Invalid', 'error': error_msg}

            # Aggregate validation results
            validation_results = {
                "Format Validation": format_valid,
                "Duplicate Records": pd.concat(duplicate_records).drop_duplicates() if duplicate_records else pd.DataFrame(),
                "Conflicting Records": pd.concat(conflicting_records).drop_duplicates() if conflicting_records else pd.DataFrame(),
                "Integrity Issues": pd.concat(integrity_issues).drop_duplicates() if integrity_issues else pd.DataFrame(),
                "Referential Integrity Issues": pd.DataFrame(),  # Placeholder if needed
                "Anomalies Detected": anomalies_detected.drop_duplicates() if not anomalies_detected.empty else pd.DataFrame()
            }

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

            # Calculate quality scores
            total_records = total_records or 1  # Avoid division by zero
            valid_records = total_records - len(validation_results["Integrity Issues"])
            schema_validation_score = (valid_records / total_records) * 100

            total_cells = total_records * len(sample_df.columns)
            total_missing = missing_counts.sum()
            missing_data_score = ((total_cells - total_missing) / total_cells) * 100

            mapping_success_scores = [stats['success_rate'] for stats in mapping_success_rates.values()]
            mapping_success_score = sum(mapping_success_scores) / len(mapping_success_scores) if mapping_success_scores else 0

            overall_quality_score = (schema_validation_score + missing_data_score + mapping_success_score) / 3

            quality_scores = {
                'Schema Validation Score': schema_validation_score,
                'Missing Data Score': missing_data_score,
                'Mapping Success Score': mapping_success_score,
                'Overall Quality Score': overall_quality_score
            }

            # Generate Reports
            report_path = os.path.join(
                output_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}_report.{report_format}"
            )

            # Create visualizations using sample_df
            figs = create_visual_summary(
                sample_df,
                phenotype_column=phenotype_column,  # Pass phenotype_column
                output_image_path=None
            )

            # Save visualizations as images
            visualization_images = []
            for idx, fig in enumerate(figs):
                image_filename = f"{os.path.splitext(os.path.basename(file_path))[0]}_visual_{idx}.png"
                image_path = os.path.join(output_dir, image_filename)
                try:
                    # Explicitly specify format and scale
                    fig.write_image(image_path, format='png', scale=2)
                    visualization_images.append(image_path)
                except Exception as e:
                    log_activity(f"Error saving image {image_filename}: {e}", level='error')

            generate_qc_report(
                validation_results,
                missing_counts,
                flagged_records_count,
                mapping_success_rates,
                visualization_images,
                impute_strategy,
                quality_scores,  # Added parameter
                report_path,
                report_format
            )
            log_activity(f"{file_path}: QC report generated at {report_path}.")
            pbar.update(5)

            log_activity(f"{file_path}: Processed data saved at {output_data_file}")

            pbar.update(5)
            pbar.close()

            result = {
                'file': file_path,
                'status': 'Processed',
                'error': None,
                'validation_results': validation_results,
                'missing_data': missing_counts,
                'flagged_records_count': flagged_records_count,
                'processed_file_path': output_data_file,
                'mapping_success_rates': mapping_success_rates,
                'visualization_images': visualization_images,
                'quality_scores': quality_scores  # Added parameter
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
    with concurrent.futures.ProcessPoolExecutor() as executor:
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
