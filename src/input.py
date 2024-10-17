import pandas as pd
import json
import csv

def read_csv(file_path):
    """
    Reads a CSV file and returns a pandas DataFrame after validating delimiter consistency.

    Args:
        file_path (str): Path to the CSV file.

    Returns:
        pd.DataFrame: DataFrame containing the CSV data.

    Raises:
        ValueError: If delimiter inconsistencies are found.
    """
    # Read the file using csv module to check delimiter consistency
    with open(file_path, 'r', newline='') as f:
        reader = csv.reader(f)
        expected_columns = None
        line_number = 0
        for row in reader:
            line_number += 1
            if expected_columns is None:
                expected_columns = len(row)
            else:
                if len(row) != expected_columns:
                    raise ValueError(
                        f"Delimiter inconsistency detected in file '{file_path}' at line {line_number}. "
                        f"Expected {expected_columns} columns but found {len(row)} columns."
                    )
    # If no inconsistencies, read the file using pandas
    return pd.read_csv(file_path)

def read_tsv(file_path):
    """
    Reads a TSV file and returns a pandas DataFrame after validating delimiter consistency.

    Args:
        file_path (str): Path to the TSV file.

    Returns:
        pd.DataFrame: DataFrame containing the TSV data.

    Raises:
        ValueError: If delimiter inconsistencies are found.
    """
    # Read the file using csv module to check delimiter consistency
    with open(file_path, 'r', newline='') as f:
        reader = csv.reader(f, delimiter='\t')
        expected_columns = None
        line_number = 0
        for row in reader:
            line_number += 1
            if expected_columns is None:
                expected_columns = len(row)
            else:
                if len(row) != expected_columns:
                    raise ValueError(
                        f"Delimiter inconsistency detected in file '{file_path}' at line {line_number}. "
                        f"Expected {expected_columns} columns but found {len(row)} columns."
                    )
    # If no inconsistencies, read the file using pandas
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
