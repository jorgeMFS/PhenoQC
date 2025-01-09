import pandas as pd
import json

def read_csv(file_path, chunksize=10000):
    """
    Reads a CSV file and returns an iterator over pandas DataFrame chunks.

    Args:
        file_path (str): Path to the CSV file.
        chunksize (int): Number of rows per chunk.

    Returns:
        Iterator[pd.DataFrame]: DataFrame chunks.
    """
    return pd.read_csv(file_path, chunksize=chunksize)

def read_tsv(file_path, chunksize=10000):
    """
    Reads a TSV file and returns an iterator over pandas DataFrame chunks.

    Args:
        file_path (str): Path to the TSV file.
        chunksize (int): Number of rows per chunk.

    Returns:
        Iterator[pd.DataFrame]: DataFrame chunks.
    """
    return pd.read_csv(file_path, sep='\t', chunksize=chunksize)

def read_json(file_path, chunksize=10000):
    """
    Reads a JSON file and returns an iterator over pandas DataFrame chunks.
    Now gracefully handles empty files or decode errors.
    """
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read().strip()
    
    if not content:
        # File is empty => yield an empty DataFrame
        yield pd.DataFrame()
        return

    try:
        data = json.loads(content)
    except json.JSONDecodeError:
        # If JSON is invalid => raise a ValueError so process_file can handle it
        raise ValueError(f"Invalid JSON file or decode error at: {file_path}")

    # If data is not a list or dict, we can handle that scenario as well
    # but typically it should be list/dict. For safety:
    if not isinstance(data, (list, dict)):
        raise ValueError(f"JSON content is not a list/dict: {file_path}")

    # Normalize JSON data into a flat table
    df = pd.json_normalize(data)

    if df.empty:
        yield pd.DataFrame()
        return

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
        Iterator[pd.DataFrame]: Data iterator for CSV/TSV/JSON.

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
