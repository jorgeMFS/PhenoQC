from jsonschema import validate, ValidationError
import pandas as pd

def validate_schema(data, schema):
    """
    Validates data against a JSON schema.
    
    Args:
        data (dict or list): Data to validate.
        schema (dict): JSON schema to validate against.
    
    Returns:
        tuple: (bool indicating if valid, error message or None)
    """
    try:
        validate(instance=data, schema=schema)
        return True, None
    except ValidationError as ve:
        return False, str(ve)

def check_required_fields(df, required_fields):
    """
    Checks if required fields are present in the DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame to check.
        required_fields (list): List of required field names.
    
    Returns:
        list: Missing fields.
    """
    missing = [field for field in required_fields if field not in df.columns]
    return missing

def check_data_types(df, expected_types):
    """
    Checks if the DataFrame columns match the expected data types.
    
    Args:
        df (pd.DataFrame): DataFrame to check.
        expected_types (dict): Dictionary mapping column names to expected data types.
    
    Returns:
        dict: Columns with mismatched data types.
    """
    mismatches = {}
    for column, expected_type in expected_types.items():
        if column in df.columns:
            if expected_type == "number":
                if not pd.api.types.is_numeric_dtype(df[column]):
                    mismatches[column] = f"Expected numeric, got {df[column].dtype}"
            elif expected_type == "string":
                if not pd.api.types.is_string_dtype(df[column]):
                    mismatches[column] = f"Expected string, got {df[column].dtype}"
            elif isinstance(expected_type, list) and "null" in expected_type:
                # Handle columns that allow null values
                non_null_type = [t for t in expected_type if t != "null"][0]
                if not pd.api.types.is_numeric_dtype(df[column]) and non_null_type == "number":
                    mismatches[column] = f"Expected numeric or null, got {df[column].dtype}"
                elif not pd.api.types.is_string_dtype(df[column]) and non_null_type == "string":
                    mismatches[column] = f"Expected string or null, got {df[column].dtype}"
    return mismatches

def perform_consistency_checks(df):
    """
    Performs consistency checks on the DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame to check.
    
    Returns:
        list: List of inconsistency messages.
    """
    inconsistencies = []
    # Example: Check for negative values in specific columns
    numeric_cols = df.select_dtypes(include=['number']).columns
    for col in numeric_cols:
        if (df[col] < 0).any():
            inconsistencies.append(f"Negative values found in column '{col}'.")
    # Add more consistency checks as needed
    return inconsistencies