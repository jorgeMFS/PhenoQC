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
    """
    Returns 'csv', 'tsv', or 'json' depending on the file extension.
    Raises ValueError if unsupported.
    """
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
    phenotype_column='Phenotype'
):
    """
    Processes a single file in chunks:
      1. Loads data by chunk.
      2. Validates (row-level + cell-level).
      3. Checks duplicates, conflicts, anomalies, etc.
      4. Imputes missing data.
      5. Maps phenotypes to ontologies.
      6. Writes processed chunk to CSV + accumulates stats.
      7. Creates a final QC report and returns a result dict.

    Returns:
        dict with keys like 'file', 'status', 'validation_results', etc.
    """
    file_type = get_file_type(file_path)
    log_activity(f"Processing file: {file_path}")
    print(f"Processing file: {file_path}")

    try:
        # Prepare a progress bar for the entire file
        with tqdm(total=100, desc=f"Processing {os.path.basename(file_path)}") as pbar:
            # 1) Load data chunk-by-chunk
            data_iterator = load_data(file_path, file_type, chunksize=chunksize)
            pbar.update(5)
            log_activity("Data loading initiated.")

            # 2) Initialize accumulators
            total_records = 0
            flagged_records_count = 0
            phenotype_terms_set = set()

            # We'll write processed data to output_data_file
            output_data_file = os.path.join(output_dir, os.path.basename(file_path))
            if os.path.exists(output_data_file):
                os.remove(output_data_file)

            write_header = True

            # For ontology mapping stats
            target_ontologies = target_ontologies or ontology_mapper.default_ontologies
            cumulative_mapping_stats = {}
            for onto_id in target_ontologies:
                cumulative_mapping_stats[onto_id] = {'total_terms': 0, 'mapped_terms': 0}

            # We'll sample up to 10k rows for visual summaries
            sample_df = pd.DataFrame()
            sample_size_per_chunk = 1000
            max_total_samples = 10000

            # For final validation results
            format_valid = True
            duplicate_records = []
            conflicting_records = []
            integrity_issues = []
            anomalies_detected = pd.DataFrame()
            missing_counts = pd.Series(dtype=int)

            # A set to track duplicates across chunks
            unique_id_set = set()

            # NEW: We'll keep a global invalid_mask for cell-level validation across chunks
            global_invalid_mask = pd.DataFrame()
            row_offset = 0  # to keep track of row indices across chunks

            chunk_progress = 80  # portion of the progress bar for chunk processing
            total_chunks = 0

            # 3) Process each chunk
            for chunk in data_iterator:
                total_chunks += 1
                nrows_chunk = len(chunk)
                if nrows_chunk == 0:
                    continue

                # Reindex the chunk so we can merge invalid_masks globally
                chunk.index = range(row_offset, row_offset + nrows_chunk)
                row_offset += nrows_chunk

                total_records += nrows_chunk

                # 3a) Validate (row-level + cell-level)
                validator = DataValidator(chunk, schema, unique_identifiers)
                chunk_results = validator.run_all_validations()

                # Was row-level format valid?
                if not chunk_results["Format Validation"]:
                    format_valid = False
                    if not chunk_results["Integrity Issues"].empty:
                        integrity_issues.append(chunk_results["Integrity Issues"])

                # Collect duplicates, conflicts, anomalies, etc.
                if not chunk_results["Duplicate Records"].empty:
                    duplicate_records.append(chunk_results["Duplicate Records"])
                if not chunk_results["Conflicting Records"].empty:
                    conflicting_records.append(chunk_results["Conflicting Records"])
                if not chunk_results["Anomalies Detected"].empty:
                    anomalies_detected = pd.concat([anomalies_detected, chunk_results["Anomalies Detected"]])

                # Merge chunk-level Integrity Issues
                if not chunk_results["Integrity Issues"].empty:
                    integrity_issues.append(chunk_results["Integrity Issues"])

                # Merge the invalid_mask from this chunk
                chunk_invalid_mask = chunk_results["Invalid Mask"]
                # Merge into global
                global_invalid_mask = pd.concat([global_invalid_mask, chunk_invalid_mask], axis=0)

                # 3b) Cross-chunk duplicates: check unique IDs
                ids_in_chunk = set(map(tuple, chunk[unique_identifiers].drop_duplicates().values.tolist()))
                duplicates_in_ids = unique_id_set.intersection(ids_in_chunk)
                if duplicates_in_ids:
                    # these are cross-chunk duplicates
                    cross_dup = chunk[chunk[unique_identifiers].apply(tuple, axis=1).isin(duplicates_in_ids)]
                    duplicate_records.append(cross_dup)
                unique_id_set.update(ids_in_chunk)

                # 3c) Missing data detection
                missing = detect_missing_data(chunk)
                missing_counts = missing_counts.add(missing, fill_value=0)

                # 3d) Flag missing data
                chunk = flag_missing_data_records(chunk)
                chunk_flagged_count = chunk['MissingDataFlag'].sum()
                flagged_records_count += chunk_flagged_count

                # 3e) Impute
                chunk = impute_missing_data(chunk, strategy=impute_strategy, field_strategies=field_strategies)
                # Re-flag after imputation
                chunk = flag_missing_data_records(chunk)

                # 3f) Collect phenotype terms
                if phenotype_column not in chunk.columns:
                    msg = f"'{phenotype_column}' column not found in chunk."
                    log_activity(f"{file_path}: {msg}", level='error')
                    pbar.close()
                    return {'file': file_path, 'status': 'Invalid', 'error': msg}
                phenotype_terms_set.update(chunk[phenotype_column].unique())

                # 3g) Ontology mapping
                terms_in_chunk = chunk[phenotype_column].unique()
                mappings = ontology_mapper.map_terms(terms_in_chunk, target_ontologies, custom_mappings)

                for onto_id in target_ontologies:
                    col_name = f"{onto_id}_ID"
                    chunk[col_name] = chunk[phenotype_column].apply(
                        lambda x: mappings.get(x, {}).get(onto_id)
                    )
                    # stats
                    mapped_count = chunk[col_name].notnull().sum()
                    cumulative_mapping_stats[onto_id]['total_terms'] += len(chunk)
                    cumulative_mapping_stats[onto_id]['mapped_terms'] += mapped_count

                # 3h) Collect sample rows for visualization
                if len(sample_df) < max_total_samples:
                    remaining = max_total_samples - len(sample_df)
                    chunk_sample_size = min(sample_size_per_chunk, remaining)
                    if len(chunk) > chunk_sample_size:
                        sample_chunk = chunk.sample(n=chunk_sample_size, random_state=42)
                    else:
                        sample_chunk = chunk.copy()
                    sample_df = pd.concat([sample_df, sample_chunk], ignore_index=True)

                # 3i) Write processed chunk to disk
                chunk.to_csv(output_data_file, mode='a', index=False, header=write_header)
                if write_header:
                    write_header = False

                # Update the progress bar proportionally to how many rows processed
                pbar.update(chunk_progress / max(1, total_records / chunksize))

            # end of for chunk in data_iterator

            pbar.update(5)  # partial progress update

            # 4) Summarize validation results
            if not format_valid:
                # combine all integrity_issues dataframes
                num_invalid = sum(len(df_part) for df_part in integrity_issues) if integrity_issues else 0
                error_msg = (
                    "Format validation failed. "
                    f"{num_invalid} record(s) do not match the JSON schema, but continuing..."
                )
                log_activity(f"{file_path}: {error_msg}", level='warning')
                final_status = 'ProcessedWithWarnings'
            else:
                error_msg = None
                final_status = 'Processed'

            # Combine duplicates, conflicts, etc.
            all_duplicates = pd.concat(duplicate_records).drop_duplicates() if duplicate_records else pd.DataFrame()
            all_conflicts = pd.concat(conflicting_records).drop_duplicates() if conflicting_records else pd.DataFrame()
            all_integrity = pd.concat(integrity_issues).drop_duplicates() if integrity_issues else pd.DataFrame()
            anomalies_detected = anomalies_detected.drop_duplicates() if not anomalies_detected.empty else pd.DataFrame()

            validation_results = {
                "Format Validation": format_valid,
                "Duplicate Records": all_duplicates,
                "Conflicting Records": all_conflicts,
                "Integrity Issues": all_integrity,
                "Referential Integrity Issues": pd.DataFrame(),  # placeholder
                "Anomalies Detected": anomalies_detected,
                # KEY: store the global invalid_mask
                "Invalid Mask": global_invalid_mask.sort_index()
            }

            # 5) Calculate ontology mapping success rates
            mapping_success_rates = {}
            for onto_id, stats in cumulative_mapping_stats.items():
                total_terms = stats['total_terms']
                mapped_terms = stats['mapped_terms']
                success_rate = (mapped_terms / total_terms) * 100 if total_terms > 0 else 0
                mapping_success_rates[onto_id] = {
                    'total_terms': total_terms,
                    'mapped_terms': mapped_terms,
                    'success_rate': success_rate
                }

            # 6) Compute quality scores
            total_records = total_records or 1
            valid_records = total_records - len(all_integrity)
            schema_validation_score = (valid_records / total_records) * 100

            total_cells = total_records * len(sample_df.columns)
            total_missing = missing_counts.sum()
            # if sample_df has no columns, avoid zero-division
            if len(sample_df.columns) == 0:
                missing_data_score = 100.0  # trivially no columns
            else:
                missing_data_score = ((total_cells - total_missing) / total_cells) * 100 if total_cells > 0 else 100.0

            # average the mapping success rates
            success_rates_list = [d['success_rate'] for d in mapping_success_rates.values()]
            mapping_success_score = sum(success_rates_list) / len(success_rates_list) if success_rates_list else 0

            overall_quality_score = (schema_validation_score + missing_data_score + mapping_success_score) / 3.0

            quality_scores = {
                'Schema Validation Score': schema_validation_score,
                'Missing Data Score': missing_data_score,
                'Mapping Success Score': mapping_success_score,
                'Overall Quality Score': overall_quality_score
            }

            # 7) Generate the QC report
            report_path = os.path.join(
                output_dir, f"{os.path.splitext(os.path.basename(file_path))[0]}_report.{report_format}"
            )

            figs = create_visual_summary(sample_df, phenotype_column=phenotype_column, output_image_path=None)

            visualization_images = []
            for idx, fig in enumerate(figs):
                image_filename = f"{os.path.splitext(os.path.basename(file_path))[0]}_visual_{idx}.png"
                image_path = os.path.join(output_dir, image_filename)
                try:
                    fig.write_image(image_path, format='png', scale=2)
                    visualization_images.append(image_path)
                except Exception as e:
                    log_activity(f"Error saving image {image_filename}: {e}", level='error')

            # build final report
            generate_qc_report(
                validation_results=validation_results,
                missing_data=missing_counts,
                flagged_records_count=flagged_records_count,
                mapping_success_rates=mapping_success_rates,
                visualization_images=visualization_images,
                impute_strategy=impute_strategy,
                quality_scores=quality_scores,
                output_path_or_buffer=report_path,
                report_format=report_format
            )
            log_activity(f"{file_path}: QC report generated at {report_path}.")
            pbar.update(5)

            log_activity(f"{file_path}: Processed data saved at {output_data_file}")
            pbar.update(5)
            pbar.close()

            # 8) Build final result object
            result = {
                'file': file_path,
                'status': final_status,
                'error': error_msg,
                'validation_results': validation_results,
                'missing_data': missing_counts,
                'flagged_records_count': flagged_records_count,
                'processed_file_path': output_data_file,
                'mapping_success_rates': mapping_success_rates,
                'visualization_images': visualization_images,
                'quality_scores': quality_scores
            }

            return result

    except Exception as e:
        log_activity(f"Error processing file {file_path}: {str(e)}", level='error')
        return {
            'file': file_path,
            'status': 'Error',
            'error': str(e)
        }

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
    chunksize=10000,
    phenotype_column='Phenotype'
):
    """
    Main entry point to process multiple files in parallel. 
    Spawns concurrent processes for each input file.
    """
    # 1) Load the schema
    with open(schema_path, 'r') as f:
        schema = json.load(f)

    # 2) Load the config
    config = load_config(config_path)

    # 3) Field-specific strategies from config
    field_strategies = config.get('imputation_strategies', {})

    # 4) Initialize the OntologyMapper
    ontology_mapper = OntologyMapper(config_path=config_path)

    # 5) Custom mappings (if any)
    custom_mappings = None
    if custom_mappings_path and os.path.exists(custom_mappings_path):
        with open(custom_mappings_path, 'r') as f:
            custom_mappings = json.load(f)

    # 6) Ensure output dir
    os.makedirs(output_dir, exist_ok=True)

    results = []
    # 7) Launch parallel jobs
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
                chunksize,
                phenotype_column
            ): file_path for file_path in files
        }

        # 8) Collect results as each future completes
        for future in tqdm(concurrent.futures.as_completed(futures), total=len(futures), desc="Batch Processing"):
            file_path = futures[future]
            try:
                result = future.result()
                results.append(result)
                # Print a quick summary
                status = result['status']
                if status == 'Processed':
                    print(f"✅ {os.path.basename(file_path)} processed successfully.")
                elif status == 'Invalid':
                    print(f"⚠️ {os.path.basename(file_path)} failed validation: {result['error']}")
                else:
                    print(f"❌ {os.path.basename(file_path)} encountered an error: {result['error']}")
            except Exception as e:
                log_activity(f"Error in processing {file_path}: {str(e)}", level='error')
                print(f"❌ Error in processing {os.path.basename(file_path)}: {str(e)}")

    return results

def collect_files(inputs, recursive=True):
    """
    Helper to gather all .csv, .tsv, .json files from a list of paths.
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
