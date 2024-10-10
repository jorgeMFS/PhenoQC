import pandas as pd
import json

def read_csv(file_path):
    """
    Reads a CSV file and returns a pandas DataFrame.
    
    Args:
        file_path (str): Path to the CSV file.
    
    Returns:
        pd.DataFrame: DataFrame containing the CSV data.
    """
    return pd.read_csv(file_path)

def read_tsv(file_path):
    """
    Reads a TSV file and returns a pandas DataFrame.
    
    Args:
        file_path (str): Path to the TSV file.
    
    Returns:
        pd.DataFrame: DataFrame containing the TSV data.
    """
    return pd.read_csv(file_path, sep='\t')

def read_json(file_path):
    """
    Reads a JSON file and returns the data as a Python dictionary.
    
    Args:
        file_path (str): Path to the JSON file.
    
    Returns:
        dict: Dictionary containing the JSON data.
    """
    with open(file_path, 'r') as f:
        return json.load(f)

def load_data(file_path, file_type):
    """
    Loads data from a file based on its type.
    
    Args:
        file_path (str): Path to the data file.
        file_type (str): Type of the file ('csv', 'tsv', 'json').
    
    Returns:
        pd.DataFrame or dict: Loaded data.
    
    Raises:
        ValueError: If the file type is unsupported.
    """
    if file_type.lower() == 'csv':
        return read_csv(file_path)
    elif file_type.lower() == 'tsv':
        return read_tsv(file_path)
    elif file_type.lower() == 'json':
        return read_json(file_path)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")