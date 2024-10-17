import pandas as pd
import json
import csv

def read_csv(file_path, chunksize=10000):
    """
    Reads a CSV file and returns an iterator over pandas DataFrame chunks after validating delimiter consistency.

    Args:
        file_path (str): Path to the CSV file.
        chunksize (int): Number of rows per chunk.

    Returns:
        Iterator[pd.DataFrame]: Iterator over DataFrame chunks.

    Raises:
        ValueError: If delimiter inconsistencies are found.
    """
    # Validate delimiter consistency
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
    # Read the file in chunks
    return pd.read_csv(file_path, chunksize=chunksize)

def read_tsv(file_path, chunksize=10000):
    """
    Reads a TSV file and returns an iterator over pandas DataFrame chunks after validating delimiter consistency.

    Args:
        file_path (str): Path to the TSV file.
        chunksize (int): Number of rows per chunk.

    Returns:
        Iterator[pd.DataFrame]: Iterator over DataFrame chunks.

    Raises:
        ValueError: If delimiter inconsistencies are found.
    """
    # Validate delimiter consistency
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
    # Read the file in chunks
    return pd.read_csv(file_path, sep='\t', chunksize=chunksize)

def read_json(file_path, chunksize=10000):
    """
    Reads a JSON file and returns an iterator over pandas DataFrame chunks.

    Args:
        file_path (str): Path to the JSON file.
        chunksize (int): Number of records per chunk.

    Yields:
        pd.DataFrame: DataFrame chunk.
    """
    with open(file_path, 'r') as f:
        data = json.load(f)
    
    # Normalize JSON data into a flat table
    df = pd.json_normalize(data)
    
    # Yield the DataFrame in chunks if necessary
    if chunksize < len(df):
        for start in range(0, len(df), chunksize):
            yield df.iloc[start:start + chunksize]
    else:
        yield df

def load_data(file_path, file_type, chunksize=10000):
    """
    Loads data from a file based on its type.

    Args:
        file_path (str): Path to the data file.
        file_type (str): Type of the file ('csv', 'tsv', 'json').
        chunksize (int): Number of rows per chunk (for CSV/TSV).

    Returns:
        Iterator[pd.DataFrame] or dict: Data iterator for CSV/TSV or dict for JSON.

    Raises:
        ValueError: If the file type is unsupported.
    """
    if file_type.lower() == 'csv':
        return read_csv(file_path, chunksize=chunksize)
    elif file_type.lower() == 'tsv':
        return read_tsv(file_path, chunksize=chunksize)
    elif file_type.lower() == 'json':
        return read_json(file_path, chunksize=chunksize)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
