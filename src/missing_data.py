import pandas as pd
import logging  # Import logging module

logging.basicConfig(level=logging.WARNING)

def detect_missing_data(df):
    """
    Detects missing data in the DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame to check for missing data.
    
    Returns:
        pd.Series: Count of missing values per column.
    """
    missing_data = df.isnull().sum()
    return missing_data[missing_data > 0]

def flag_missing_data_records(df):
    """
    Flags records with missing data for manual review.
    
    Args:
        df (pd.DataFrame): DataFrame to flag.
    
    Returns:
        pd.DataFrame: DataFrame with an additional 'MissingDataFlag' column.
    """
    df['MissingDataFlag'] = df.isnull().any(axis=1)
    return df

def impute_missing_data(df, strategy='mean', field_strategies=None):
    """
    Imputes missing data in the DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame to impute.
        strategy (str): Default imputation strategy ('mean', 'median', 'mode').
        field_strategies (dict): Dictionary of column-specific imputation strategies.
    
    Returns:
        pd.DataFrame: DataFrame with imputed values.
    """
    for column in df.columns:
        col_missing = df[column].isnull().sum()
        if col_missing > 0:
            # Determine the strategy for the current column
            col_strategy = field_strategies.get(column, strategy) if field_strategies else strategy

            if col_strategy == 'mean':
                if pd.api.types.is_numeric_dtype(df[column]):
                    df[column] = df[column].fillna(df[column].mean())
                else:
                    logging.warning(f"Mean imputation not applicable for non-numeric column '{column}'.")
            elif col_strategy == 'median':
                if pd.api.types.is_numeric_dtype(df[column]):
                    df[column] = df[column].fillna(df[column].median())
                else:
                    logging.warning(f"Median imputation not applicable for non-numeric column '{column}'.")
            elif col_strategy == 'mode':
                mode_value = df[column].mode()
                if not mode_value.empty:
                    df[column] = df[column].fillna(mode_value[0])
                else:
                    logging.warning(f"No mode found for column '{column}'. Unable to impute.")
            else:
                logging.warning(f"Unknown imputation strategy '{col_strategy}' for column '{column}'. Skipping imputation.")
    return df
