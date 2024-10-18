import pandas as pd
import logging  # Import logging module
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import KNNImputer, IterativeImputer
from fancyimpute import IterativeSVD

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
    Imputes missing data in the DataFrame using specified strategies.
    
    Args:
        df (pd.DataFrame): DataFrame to impute.
        strategy (str): Default imputation strategy ('mean', 'median', 'mode', 'knn', 'mice', 'svd', 'none').
        field_strategies (dict): Dictionary of column-specific imputation strategies.
    
    Returns:
        pd.DataFrame: DataFrame with imputed values.
    """
    if strategy == 'none':
        logging.info("No imputation strategy selected. Skipping imputation.")
        return df.copy()
    
    df_imputed = df.copy()
    
    # Identify numeric columns for advanced imputation
    numeric_cols = df_imputed.select_dtypes(include=['number']).columns.tolist()
    
    # Collect columns by their strategies
    strategies_columns = {
        'mean': [],
        'median': [],
        'mode': [],
        'knn': [],
        'mice': [],
        'svd': []
    }
    
    for column in df_imputed.columns:
        col_missing = df_imputed[column].isnull().sum()
        if col_missing > 0:
            # Determine the strategy for the current column
            col_strategy = field_strategies.get(column, strategy) if field_strategies else strategy
            if col_strategy in strategies_columns:
                strategies_columns[col_strategy].append(column)
            else:
                logging.warning(f"Unknown imputation strategy '{col_strategy}' for column '{column}'. Skipping imputation.")
    
    # Apply 'mean' strategy
    for column in strategies_columns['mean']:
        if pd.api.types.is_numeric_dtype(df_imputed[column]):
            df_imputed[column] = df_imputed[column].fillna(df_imputed[column].mean())
        else:
            logging.warning(f"Mean imputation not applicable for non-numeric column '{column}'.")
    
    # Apply 'median' strategy
    for column in strategies_columns['median']:
        if pd.api.types.is_numeric_dtype(df_imputed[column]):
            df_imputed[column] = df_imputed[column].fillna(df_imputed[column].median())
        else:
            logging.warning(f"Median imputation not applicable for non-numeric column '{column}'.")
           
    # Apply 'mode' strategy
    for column in strategies_columns['mode']:
        if pd.api.types.is_numeric_dtype(df_imputed[column]):
            logging.warning(f"Mode imputation not applicable for numeric column '{column}'. Skipping imputation.")
            continue
        mode_value = df_imputed[column].mode()
        if not mode_value.empty:
            df_imputed[column] = df_imputed[column].fillna(mode_value[0])
        else:
            logging.warning(f"No mode found for column '{column}'. Unable to impute.")
    
    # Apply 'knn' strategy
    if strategies_columns['knn']:
        knn_columns = [col for col in strategies_columns['knn'] if col in numeric_cols]
        if knn_columns:
            imputer = KNNImputer(n_neighbors=5)
            df_imputed[knn_columns] = imputer.fit_transform(df_imputed[knn_columns])
        else:
            logging.warning("No numeric columns for KNN imputation.")
    
    # Apply 'mice' strategy
    if strategies_columns['mice']:
        mice_columns = [col for col in strategies_columns['mice'] if col in numeric_cols]
        if mice_columns:
            imputer = IterativeImputer(random_state=0)
            df_imputed[mice_columns] = imputer.fit_transform(df_imputed[mice_columns])
        else:
            logging.warning("No numeric columns for MICE imputation.")
    
    # Apply 'svd' strategy
    if strategies_columns['svd']:
        svd_columns = [col for col in strategies_columns['svd'] if col in numeric_cols]
        if svd_columns:
            n_rows, n_cols = df_imputed[svd_columns].shape
            k = min(n_rows, n_cols) - 1
            if k < 1:
                logging.warning(f"Cannot perform SVD imputation on columns {svd_columns} with sufficient dimensions. Skipping imputation.")
            else:
                try:
                    imputer = IterativeSVD(rank=k)
                    df_imputed[svd_columns] = imputer.fit_transform(df_imputed[svd_columns])
                except TypeError as e:
                    logging.error(f"Error initializing IterativeSVD: {e}")
        else:
            logging.warning("No numeric columns for SVD imputation.")
    
    return df_imputed
