import pandas as pd
import logging
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
    Imputes missing data in the DataFrame using specified strategies,
    but only when appropriate (e.g., numeric columns for mean/median).
    
    Args:
        df (pd.DataFrame): DataFrame to impute.
        strategy (str): Default imputation strategy 
                        ('mean', 'median', 'mode', 'knn', 'mice', 'svd', 'none').
        field_strategies (dict): Dictionary of column-specific imputation strategies.
                                 E.g. {"Height_cm": "median", "CategoryCol": "mode"}
    
    Returns:
        pd.DataFrame: DataFrame with imputed values.
    """
    if strategy == 'none':
        logging.info("No imputation strategy selected. Skipping imputation.")
        return df.copy()
    
    df_imputed = df.copy()
    
    # Identify numeric columns (for possible numeric-based imputations)
    numeric_cols = df_imputed.select_dtypes(include=['number']).columns.tolist()
    
    # We'll track columns by the final chosen strategy
    # to handle them in one pass or in specialized blocks.
    strategies_columns = {
        'mean': [],
        'median': [],
        'mode': [],
        'knn': [],
        'mice': [],
        'svd': []
    }
    
    # --- Determine each column's strategy (either from field_strategies or the default) ---
    for column in df_imputed.columns:
        # How many missing cells in this column?
        col_missing = df_imputed[column].isnull().sum()
        if col_missing == 0:
            # No missing -> skip
            continue
        
        # Determine the strategy for the current column
        col_strategy = field_strategies.get(column, strategy) if field_strategies else strategy
        
        # If this strategy name is known, add the column there; else warn & skip
        if col_strategy in strategies_columns:
            strategies_columns[col_strategy].append(column)
        else:
            logging.warning(
                f"Unknown imputation strategy '{col_strategy}' for column '{column}'. "
                "Skipping imputation for that column."
            )
    
    # --- Apply the simpler strategies first (mean, median, mode) on the relevant columns ---
    
    # 1) Mean
    for column in strategies_columns['mean']:
        if pd.api.types.is_numeric_dtype(df_imputed[column]):
            df_imputed[column] = df_imputed[column].fillna(df_imputed[column].mean())
        else:
            logging.warning(
                f"Mean imputation not applicable for non-numeric column '{column}'. Skipping."
            )
    
    # 2) Median
    for column in strategies_columns['median']:
        if pd.api.types.is_numeric_dtype(df_imputed[column]):
            df_imputed[column] = df_imputed[column].fillna(df_imputed[column].median())
        else:
            logging.warning(
                f"Median imputation not applicable for non-numeric column '{column}'. Skipping."
            )
           
    # 3) Mode
    for column in strategies_columns['mode']:
        # Mode can be applied to numeric or non-numeric,
        # but typically it’s used for categorical/string columns.
        # We'll still allow it if you want to handle numeric columns via 'mode'.
        mode_vals = df_imputed[column].mode(dropna=True)
        if not mode_vals.empty:
            df_imputed[column] = df_imputed[column].fillna(mode_vals[0])
        else:
            logging.warning(
                f"No mode found for column '{column}'. Unable to impute with mode."
            )
    
    # --- Advanced imputation (KNN, MICE, SVD) typically for numeric columns only ---
    
    # 4) KNN
    if strategies_columns['knn']:
        # Filter to numeric columns only
        knn_columns = [col for col in strategies_columns['knn'] if col in numeric_cols]
        if knn_columns:
            # Apply KNN on that subset of numeric columns
            imputer = KNNImputer(n_neighbors=5)
            df_imputed[knn_columns] = imputer.fit_transform(df_imputed[knn_columns])
        else:
            logging.warning("No numeric columns found for KNN imputation.")
    
    # 5) MICE (IterativeImputer)
    if strategies_columns['mice']:
        mice_columns = [col for col in strategies_columns['mice'] if col in numeric_cols]
        if mice_columns:
            imputer = IterativeImputer(random_state=0)
            df_imputed[mice_columns] = imputer.fit_transform(df_imputed[mice_columns])
        else:
            logging.warning("No numeric columns found for MICE imputation.")
    
    # 6) SVD (IterativeSVD from fancyimpute)
    if strategies_columns['svd']:
        svd_columns = [col for col in strategies_columns['svd'] if col in numeric_cols]
        if svd_columns:
            n_rows, n_cols = df_imputed[svd_columns].shape
            k = min(n_rows, n_cols) - 1
            if k < 1:
                logging.warning(
                    f"Cannot perform SVD imputation on columns {svd_columns} "
                    "due to insufficient dimensions. Skipping."
                )
            else:
                try:
                    imputer = IterativeSVD(rank=k)
                    df_imputed[svd_columns] = imputer.fit_transform(df_imputed[svd_columns])
                except TypeError as e:
                    logging.error(
                        f"Error initializing IterativeSVD for columns {svd_columns}: {e}"
                    )
        else:
            logging.warning("No numeric columns found for SVD imputation.")
    
    return df_imputed
