import concurrent.futures
import os
import json
import pandas as pd 
from input import load_data
from validation import validate_schema, check_required_fields, check_data_types, perform_consistency_checks
from mapping import fetch_hpo_terms, map_to_hpo, load_custom_mappings
from missing_data import detect_missing_data, impute_missing_data
from reporting import generate_qc_report, create_visual_summary
from logging_module import log_activity
from configuration import load_config  # Ensure configuration module is imported

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

def process_file(file_path, schema, hpo_terms, custom_mappings=None, impute_strategy='mean', output_dir='reports'):
    file_type = get_file_type(file_path)
    log_activity(f"Processing file: {file_path}")
    print(f"Processing file: {file_path}")
    print(f"File type: {file_type}")
    print(f"Impute strategy: {impute_strategy}")

    try:
        data = load_data(file_path, file_type)
        log_activity("Data loaded successfully.")
        
        df = pd.DataFrame(data) if isinstance(data, list) else data
        
        # Validate schema for each row
        for index, row in df.iterrows():
            is_valid, error = validate_schema(row.to_dict(), schema)
            if not is_valid:
                log_activity(f"Schema validation failed for row {index} in {file_path}: {error}", level='error')
                return {'file': file_path, 'status': 'Invalid', 'error': f"Row {index}: {error}"}
        log_activity("Schema validation passed for all rows.")
        
        # Check required fields and data types
        required_fields = schema.get("required", [])
        missing_fields = check_required_fields(df, required_fields)
        if missing_fields:
            log_activity(f"Missing required fields in {file_path}: {missing_fields}", level='error')
            return {'file': file_path, 'status': 'Invalid', 'error': f"Missing fields: {missing_fields}"}
        
        expected_types = {prop: schema['properties'][prop]['type'] for prop in schema['properties']}
        mismatches = check_data_types(df, expected_types)
        if mismatches:
            log_activity(f"Data type mismatches in {file_path}: {mismatches}", level='error')
            return {'file': file_path, 'status': 'Invalid', 'error': f"Type mismatches: {mismatches}"}
        
        # Perform consistency checks
        inconsistencies = perform_consistency_checks(df)
        if inconsistencies:
            log_activity(f"Consistency issues in {file_path}: {inconsistencies}", level='warning')
        
        # Ontology mapping
        if 'Phenotype' in df.columns:
            df['HPO_ID'] = df['Phenotype'].apply(lambda x: map_to_hpo(x, hpo_terms, custom_mappings))
            unmapped = df[df['HPO_ID'].isnull()]
            if not unmapped.empty:
                log_activity(f"Unmapped phenotypes in {file_path}: {unmapped['Phenotype'].unique()}", level='warning')
        
        # Missing data handling
        missing = detect_missing_data(df)
        if not missing.empty:
            if impute_strategy:
                df = impute_missing_data(df, strategy=impute_strategy)
                log_activity(f"Missing data imputed using {impute_strategy} strategy.")
            else:
                log_activity(f"Missing data detected in {file_path}: {missing.to_dict()}", level='warning')
        
        # Generate report
        report_file = os.path.join(output_dir, os.path.splitext(os.path.basename(file_path))[0] + "_report.pdf")
        generate_qc_report(df, missing, report_file)
        create_visual_summary(missing, os.path.join(output_dir, os.path.splitext(os.path.basename(file_path))[0] + "_missing_data.png"))
        log_activity(f"Report generated at {report_file}")
        
        # Save the cleaned DataFrame if needed
        output_data_file = os.path.join(output_dir, os.path.basename(file_path))
        df.to_csv(output_data_file, index=False)
        log_activity(f"Processed data saved at {output_data_file}")
        
        return {'file': file_path, 'status': 'Processed', 'error': None}
    
    except Exception as e:
        log_activity(f"Error processing file {file_path}: {str(e)}", level='error')
        return {'file': file_path, 'status': 'Error', 'error': str(e)}

def batch_process(files, schema_path, hpo_terms_path, custom_mappings_path=None, impute_strategy='mean', output_dir='reports'):
    """
    Processes multiple phenotypic data files, each potentially of different types.
    
    Args:
        files (list): List of file paths to process.
        schema_path (str): Path to the JSON schema file.
        hpo_terms_path (str): Path to the HPO terms JSON file.
        custom_mappings_path (str, optional): Path to the custom mapping JSON file.
        impute_strategy (str): Strategy for imputing missing data.
        output_dir (str): Directory to save reports and processed data.
    
    Returns:
        list: List of results for each file.
    """
    # Load schema
    with open(schema_path, 'r') as f:
        schema = json.load(f)
    
    # Fetch HPO terms
    hpo_terms = fetch_hpo_terms(local_file_path=hpo_terms_path)
    
    # Load custom mappings if provided
    custom_mappings = load_custom_mappings(custom_mappings_path) if custom_mappings_path else None
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    results = []
    with concurrent.futures.ThreadPoolExecutor() as executor:
        futures = [executor.submit(process_file, file_path, schema, hpo_terms, custom_mappings, impute_strategy, output_dir) for file_path in files]
        for future in concurrent.futures.as_completed(futures):
            result = future.result()
            results.append(result)
            if result['status'] == 'Processed':
                print(f"✅ {os.path.basename(result['file'])} processed successfully.")
            elif result['status'] == 'Invalid':
                print(f"⚠️ {os.path.basename(result['file'])} failed validation: {result['error']}")
            else:
                print(f"❌ {os.path.basename(result['file'])} encountered an error: {result['error']}")
    
    return results