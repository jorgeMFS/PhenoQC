import os
import json
import hashlib
import pandas as pd
from tqdm import tqdm
import concurrent.futures

from input import load_data
from validation import DataValidator
from mapping import OntologyMapper
from configuration import load_config
from logging_module import log_activity, setup_logging
from reporting import generate_qc_report, create_visual_summary
from missing_data import detect_missing_data, impute_missing_data, flag_missing_data_records



def child_process_run(
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
    phenotype_columns,
    log_file_for_children
):
    """
    This top-level function is what each child process calls.
    We do the logging re-init in append mode, then run process_file.
    """
    setup_logging(log_file=log_file_for_children, mode='a')
    return process_file(
        file_path=file_path,
        schema=schema,
        ontology_mapper=ontology_mapper,
        unique_identifiers=unique_identifiers,
        custom_mappings=custom_mappings,
        impute_strategy=impute_strategy,
        field_strategies=field_strategies,
        output_dir=output_dir,
        target_ontologies=target_ontologies,
        report_format=report_format,
        chunksize=chunksize,
        phenotype_columns=phenotype_columns
    )

def unique_output_name(file_path, output_dir, suffix='.csv'):
    """
    Creates a unique output filename using:
     - The original file's *base name* (not the entire path),
     - A short 5-char hash based on that base name (to avoid collisions),
     - The original extension (e.g. .json -> '_json'),
     - And finally the desired suffix (.csv, _report.pdf, etc.).

    Example output:
       sample_data.json  --> sample_data_abc12_json.csv
    """
    # Only take the base name to avoid huge temp paths
    just_name = os.path.basename(file_path)  # e.g. "sample_data.json"
    
    # Short hash from that base name
    short_hash = hashlib.md5(just_name.encode('utf-8')).hexdigest()[:5]

    base_no_ext, orig_ext = os.path.splitext(just_name)
    ext_no_dot = orig_ext.lstrip('.')  # e.g. "json"

    final_name = f"{base_no_ext}_{short_hash}_{ext_no_dot}{suffix}"
    return os.path.join(output_dir, final_name)

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
    phenotype_columns=None
):
    """
    Modified process_file that:
    1) Performs row-level validation and gathers IntegrityIssues with any invalid strings (e.g. "Thirty"),
    2) THEN coerces numeric columns even if schema["type"] is a list like ["number","null"],
    3) THEN does missing-data imputation (mean, median, etc.),
    ensuring both "Thirty" is captured in IntegrityIssues and Measurement is numeric for mean imputation.
    """
    from tqdm import tqdm
    import os
    import pandas as pd
    from logging_module import log_activity
    from input import load_data
    from validation import DataValidator
    from missing_data import detect_missing_data, impute_missing_data, flag_missing_data_records
    from reporting import generate_qc_report, create_visual_summary
    import hashlib
    import json

    def unique_output_name(file_path, output_dir, suffix='.csv'):
        just_name = os.path.basename(file_path)
        short_hash = hashlib.md5(just_name.encode('utf-8')).hexdigest()[:5]
        base_no_ext, orig_ext = os.path.splitext(just_name)
        ext_no_dot = orig_ext.lstrip('.')
        final_name = f"{base_no_ext}_{short_hash}_{ext_no_dot}{suffix}"
        return os.path.join(output_dir, final_name)

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

    file_type = get_file_type(file_path)
    log_activity(f"[ChildProcess] Starting on: {file_path}", level='info')

    final_status = 'Processed'
    error_msg = None

    try:
        with tqdm(total=100, desc=f"Processing {os.path.basename(file_path)}") as pbar:
            # 1) Attempt to load data in chunks
            try:
                data_iterator = load_data(file_path, file_type, chunksize=chunksize)
            except Exception as e:
                final_status = 'ProcessedWithWarnings'
                error_msg = f"Could not load data from {file_path}: {str(e)}"
                log_activity(f"{file_path}: {error_msg}", level='warning')
                data_iterator = []

            pbar.update(5)
            log_activity("Data loading initiated.")

            all_chunks = []
            try:
                first_chunk = next(data_iterator, None)
            except StopIteration:
                final_status = 'ProcessedWithWarnings'
                if not error_msg:
                    error_msg = f"No data found in {file_path}."
                log_activity(f"{file_path}: {error_msg}", level='warning')
                first_chunk = None
            except Exception as e:
                final_status = 'ProcessedWithWarnings'
                if not error_msg:
                    error_msg = f"Error reading first chunk: {str(e)}"
                log_activity(f"{file_path}: {error_msg}", level='warning')
                first_chunk = None

            if first_chunk is not None and not first_chunk.empty:
                all_chunks.append(first_chunk)
                for c in data_iterator:
                    all_chunks.append(c)
            else:
                if not error_msg:
                    error_msg = f"{file_path} is empty or has no valid rows."
                final_status = 'ProcessedWithWarnings'
                log_activity(f"{file_path}: {error_msg}", level='warning')

            total_records = 0
            flagged_records_count = 0
            sample_df = pd.DataFrame()
            sample_size_per_chunk = 1000
            max_total_samples = 10000

            output_data_file = unique_output_name(file_path, output_dir, suffix='.csv')
            if os.path.exists(output_data_file):
                os.remove(output_data_file)
            write_header = True

            # Default fallback if phenotype_columns not provided
            if phenotype_columns is None:
                phenotype_columns = {
                    "Phenotype": ["HPO"],
                    "PrimaryPhenotype": ["HPO"],
                    "DiseaseCode": ["DO"],
                    "TertiaryPhenotype": ["MPO"]
                }

            # Track ontology stats
            cumulative_mapping_stats = {}
            for colname, onts in phenotype_columns.items():
                for onto_id in onts:
                    if onto_id not in cumulative_mapping_stats:
                        cumulative_mapping_stats[onto_id] = {'total_terms': 0, 'mapped_terms': 0}

            format_valid = True
            duplicate_records = []
            conflicting_records = []
            integrity_issues = []
            anomalies_detected = pd.DataFrame()
            missing_counts = pd.Series(dtype=int)
            unique_id_set = set()
            global_invalid_mask = pd.DataFrame()
            row_offset = 0
            chunk_progress = 80

            schema_fail_indices_global = set()

            # 2) Process each chunk
            for chunk in all_chunks:
                if chunk is None or chunk.empty:
                    continue

                nrows_chunk = len(chunk)
                if nrows_chunk == 0:
                    continue

                # Offset row indices so each chunk has unique index
                chunk.index = range(row_offset, row_offset + nrows_chunk)
                row_offset += nrows_chunk
                total_records += nrows_chunk

                # A) Validate row/cell first (capture "Thirty" etc.)
                try:
                    validator = DataValidator(chunk, schema, unique_identifiers)
                    chunk_results = validator.run_all_validations()
                except KeyError as e:
                    missing_col = str(e).strip("'")
                    required_cols = schema.get("required", [])
                    if (missing_col in required_cols) or (missing_col in unique_identifiers):
                        final_status = 'ProcessedWithWarnings'
                        msg = (f"Missing required/unique col '{missing_col}' => warnings.")
                        log_activity(f"{file_path}: {msg}", level='warning')
                        chunk_results = {
                            "Format Validation": False,
                            "Duplicate Records": pd.DataFrame(),
                            "Conflicting Records": pd.DataFrame(),
                            "Integrity Issues": pd.DataFrame(),
                            "Referential Integrity Issues": pd.DataFrame(),
                            "Anomalies Detected": pd.DataFrame(),
                            "Invalid Mask": pd.DataFrame(False, index=chunk.index, columns=chunk.columns)
                        }
                    else:
                        # optional column missing => ignore
                        log_activity(f"Skipping optional column '{missing_col}'.", level='info')
                        safe_ids = [col for col in unique_identifiers if col != missing_col]
                        validator = DataValidator(chunk, schema, safe_ids)
                        chunk_results = validator.run_all_validations()
                except Exception as ex:
                    final_status = 'ProcessedWithWarnings'
                    msg2 = f"Error during validation: {str(ex)}"
                    log_activity(f"{file_path}: {msg2}", level='warning')
                    chunk_results = {
                        "Format Validation": False,
                        "Duplicate Records": pd.DataFrame(),
                        "Conflicting Records": pd.DataFrame(),
                        "Integrity Issues": pd.DataFrame(),
                        "Referential Integrity Issues": pd.DataFrame(),
                        "Anomalies Detected": pd.DataFrame(),
                        "Invalid Mask": pd.DataFrame(False, index=chunk.index, columns=chunk.columns)
                    }

                if 'SchemaViolationFlag' in chunk.columns:
                    fails_in_chunk = chunk.index[chunk['SchemaViolationFlag'] == True]
                    for row_id in fails_in_chunk:
                        schema_fail_indices_global.add(row_id)

                if not chunk_results["Format Validation"]:
                    format_valid = False
                    if not chunk_results["Integrity Issues"].empty:
                        integrity_issues.append(chunk_results["Integrity Issues"])

                if not chunk_results["Duplicate Records"].empty:
                    duplicate_records.append(chunk_results["Duplicate Records"])

                if not chunk_results["Conflicting Records"].empty:
                    conflicting_records.append(chunk_results["Conflicting Records"])

                if not chunk_results["Anomalies Detected"].empty:
                    anomalies_detected = pd.concat([anomalies_detected, chunk_results["Anomalies Detected"]])

                if not chunk_results["Integrity Issues"].empty:
                    integrity_issues.append(chunk_results["Integrity Issues"])

                # Merge invalid mask
                chunk_invalid_mask = chunk_results["Invalid Mask"].reindex(index=chunk.index, fill_value=False)
                all_cols = sorted(set(global_invalid_mask.columns) | set(chunk_invalid_mask.columns))
                global_invalid_mask = global_invalid_mask.reindex(columns=all_cols, fill_value=False)
                chunk_invalid_mask = chunk_invalid_mask.reindex(columns=all_cols, fill_value=False)
                global_invalid_mask = pd.concat([global_invalid_mask, chunk_invalid_mask], axis=0)

                # B) Duplicates across chunks
                if unique_identifiers:
                    ids_in_chunk = set(map(tuple, chunk[unique_identifiers].drop_duplicates().values.tolist()))
                    duplicates_in_ids = unique_id_set.intersection(ids_in_chunk)
                    if duplicates_in_ids:
                        cross_dup = chunk[chunk[unique_identifiers].apply(tuple, axis=1).isin(duplicates_in_ids)]
                        duplicate_records.append(cross_dup)
                    unique_id_set.update(ids_in_chunk)

                # C) Now coerce numeric columns, even if type is ["number","null"].
                for col, col_rules in schema.get("properties", {}).items():
                    # Handle multiple or single type
                    schema_type = col_rules.get("type")
                    if isinstance(schema_type, list):
                        is_numeric = "number" in schema_type
                    else:
                        is_numeric = (schema_type == "number")

                    if is_numeric and col in chunk.columns:
                        chunk[col] = pd.to_numeric(chunk[col], errors='coerce')

                # D) Missing data + Imputation
                from missing_data import detect_missing_data, flag_missing_data_records, impute_missing_data
                missing_per_col = detect_missing_data(chunk)
                missing_counts = missing_counts.add(missing_per_col, fill_value=0)

                chunk = flag_missing_data_records(chunk)
                flagged_records_count += chunk['MissingDataFlag'].sum()

                try:
                    chunk = impute_missing_data(chunk, strategy=impute_strategy, field_strategies=field_strategies)
                except Exception as ex_impute:
                    final_status = 'ProcessedWithWarnings'
                    msg3 = f"Error in imputation: {str(ex_impute)}"
                    log_activity(f"{file_path}: {msg3}", level='warning')

                # Re-flag after imputation
                chunk = flag_missing_data_records(chunk)

                # E) Ontology mapping
                for colname, ontologies in phenotype_columns.items():
                    if colname not in chunk.columns:
                        log_activity(f"Skipping optional column '{colname}' (not present).", level='info')
                        continue

                    terms_in_chunk = chunk[colname].dropna().unique()
                    if len(terms_in_chunk) == 0:
                        continue

                    mappings = ontology_mapper.map_terms(terms_in_chunk, ontologies, custom_mappings)
                    for onto_id in ontologies:
                        col_onto = f"{onto_id}_ID"
                        chunk[col_onto] = chunk[colname].map(
                            lambda x: mappings.get(str(x), {}).get(onto_id) if pd.notnull(x) else None
                        )
                        if onto_id not in cumulative_mapping_stats:
                            cumulative_mapping_stats[onto_id] = {'total_terms': 0, 'mapped_terms': 0}
                        valid_terms = [t for t in terms_in_chunk if pd.notnull(t)]
                        cumulative_mapping_stats[onto_id]['total_terms'] += len(valid_terms)
                        cumulative_mapping_stats[onto_id]['mapped_terms'] += sum(
                            1 for t in valid_terms if mappings.get(str(t), {}).get(onto_id) is not None
                        )

                # F) Gather a sample for the final QC summary
                if len(sample_df) < max_total_samples:
                    remaining = max_total_samples - len(sample_df)
                    chunk_sample_size = min(sample_size_per_chunk, remaining)
                    if len(chunk) > chunk_sample_size:
                        sample_chunk = chunk.sample(n=chunk_sample_size, random_state=42)
                    else:
                        sample_chunk = chunk.copy()
                    sample_df = pd.concat([sample_df, sample_chunk], ignore_index=True)

                # G) Write chunk to final CSV
                try:
                    chunk.to_csv(output_data_file, mode='a', index=False, header=write_header)
                    if write_header:
                        write_header = False
                except Exception as ex_csv:
                    final_status = 'ProcessedWithWarnings'
                    log_activity(f"Error writing CSV output: {str(ex_csv)}", level='warning')

                chunk_ratio = max(1, total_records / chunksize)
                pbar.update(chunk_progress / chunk_ratio)

            # 3) Summaries
            if not format_valid:
                num_invalid_integrity = sum(len(dfp) for dfp in integrity_issues) if integrity_issues else 0
                msg4 = f"Format validation failed. {num_invalid_integrity} record(s) do not match the JSON schema."
                log_activity(f"{file_path}: {msg4}", level='warning')
                if error_msg:
                    error_msg += f" | {msg4}"
                else:
                    error_msg = msg4
                final_status = 'ProcessedWithWarnings'

            dup_df = pd.concat(duplicate_records).drop_duplicates() if duplicate_records else pd.DataFrame()
            conf_df = pd.concat(conflicting_records).drop_duplicates() if conflicting_records else pd.DataFrame()
            int_issues_df = pd.concat(integrity_issues).drop_duplicates() if integrity_issues else pd.DataFrame()
            anomalies_detected = anomalies_detected.drop_duplicates() if not anomalies_detected.empty else pd.DataFrame()

            validation_results = {
                "Format Validation": format_valid,
                "Duplicate Records": dup_df,
                "Conflicting Records": conf_df,
                "Integrity Issues": int_issues_df,
                "Referential Integrity Issues": pd.DataFrame(),
                "Anomalies Detected": anomalies_detected,
                "Invalid Mask": global_invalid_mask.sort_index()
            }

            # Mapping stats
            mapping_success_rates = {}
            for onto_id, stats in cumulative_mapping_stats.items():
                total_terms = stats['total_terms']
                mapped_terms = stats['mapped_terms']
                success_rate = (mapped_terms / total_terms * 100) if total_terms > 0 else 0
                mapping_success_rates[onto_id] = {
                    'total_terms': total_terms,
                    'mapped_terms': mapped_terms,
                    'success_rate': success_rate
                }

            total_records = total_records or 1
            num_schema_fails = len(schema_fail_indices_global)
            valid_recs_for_schema = total_records - num_schema_fails
            schema_validation_score = (valid_recs_for_schema / total_records) * 100

            total_cells = total_records * len(sample_df.columns)
            total_missing = missing_counts.sum()
            if len(sample_df.columns) == 0:
                missing_data_score = 100.0
            else:
                missing_data_score = (
                    (total_cells - total_missing) / total_cells * 100
                ) if total_cells > 0 else 100.0

            sr_list = [v['success_rate'] for v in mapping_success_rates.values()]
            mapping_success_score = sum(sr_list)/len(sr_list) if sr_list else 0
            overall_quality_score = (
                schema_validation_score + missing_data_score + mapping_success_score
            ) / 3.0

            quality_scores = {
                'Schema Validation Score': schema_validation_score,
                'Missing Data Score': missing_data_score,
                'Mapping Success Score': mapping_success_score,
                'Overall Quality Score': overall_quality_score
            }

            # 4) Generate QC report
            report_path = unique_output_name(file_path, output_dir, suffix='_report.pdf')
            figs = create_visual_summary(sample_df, phenotype_columns=phenotype_columns, output_image_path=None)
            visualization_images = []
            for idx, fig in enumerate(figs):
                image_filename = f"{os.path.splitext(os.path.basename(file_path))[0]}_visual_{idx}.png"
                image_path = os.path.join(output_dir, image_filename)
                try:
                    fig.write_image(image_path, format='png', scale=2)
                    visualization_images.append(image_path)
                except Exception as e:
                    log_activity(f"Error saving image {image_filename}: {e}", level='error')

            base_display_name = os.path.basename(file_path)
            try:
                generate_qc_report(
                    validation_results=validation_results,
                    missing_data=missing_counts,
                    flagged_records_count=flagged_records_count,
                    mapping_success_rates=mapping_success_rates,
                    visualization_images=visualization_images,
                    impute_strategy=impute_strategy,
                    quality_scores=quality_scores,
                    output_path_or_buffer=report_path,
                    report_format=report_format,
                    file_identifier=base_display_name
                )
                log_activity(f"{file_path}: QC report generated at {report_path}.")
            except Exception as e:
                log_activity(f"Failed to generate PDF for {file_path}: {e}", level='warning')

            pbar.update(5)
            log_activity(f"{file_path}: Processed data saved at {output_data_file}")
            pbar.update(5)
            pbar.close()

            return {
                'file': file_path,
                'status': final_status,
                'error': error_msg,
                'validation_results': validation_results,
                'missing_data': missing_counts,
                'flagged_records_count': flagged_records_count,
                'processed_file_path': output_data_file,
                'report_path': report_path,
                'mapping_success_rates': mapping_success_rates,
                'visualization_images': visualization_images,
                'quality_scores': quality_scores
            }

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
    phenotype_columns=None,
    phenotype_column=None,
    log_file_for_children=None
):
    log_activity(f"[ParentProcess] Starting on: {files}", level='info')

    # 1) Load the schema
    with open(schema_path) as f:
        schema = json.load(f)

    # 2) Load config
    config = load_config(config_path)

    # 3) Create OntologyMapper
    ontology_mapper = OntologyMapper(config)

    # 4) Load custom mappings
    custom_mappings = None
    if custom_mappings_path:
        with open(custom_mappings_path) as f:
            custom_mappings = json.load(f)

    # Convert old style to new style if needed
    if phenotype_column and not phenotype_columns:
        phenotype_columns = {phenotype_column: ["HPO"]}

    # 5) In parallel, call child_process_run
    results = []
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = []
        for file_path in files:
            # Pass all needed args including log_file_for_children
            future = executor.submit(
                child_process_run,
                file_path,
                schema,
                ontology_mapper,
                unique_identifiers,
                custom_mappings,
                impute_strategy,
                None,  # field_strategies if you have them
                output_dir,
                target_ontologies,
                report_format,
                chunksize,
                phenotype_columns,
                log_file_for_children
            )
            futures.append(future)

        for future in concurrent.futures.as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                log_activity(f"Error in batch processing: {str(e)}", level='error')
                # Return a dict, same shape as success
                dummy_result = {
                    'file': "<Unknown>",
                    'status': 'Error',
                    'error': str(e)
                }
                results.append(dummy_result)

    return results