from jsonschema import validate, ValidationError

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
        return False, ve.message

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
            if not pd.api.types.is_dtype_equal(df[column].dtype, expected_type):
                mismatches[column] = str(expected_type)
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