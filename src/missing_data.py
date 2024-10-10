def detect_missing_data(df):
    """
    Detects missing data in the DataFrame.
    
    Args:
        df (pd.DataFrame): DataFrame to analyze.
    
    Returns:
        pd.Series: Series with counts of missing data per column.
    """
    missing = df.isnull().sum()
    return missing[missing > 0]

def impute_missing_data(df, strategy='mean'):
    """
    Imputes missing data in the DataFrame based on the specified strategy.
    
    Args:
        df (pd.DataFrame): DataFrame with missing data.
        strategy (str): Imputation strategy ('mean', 'median').
    
    Returns:
        pd.DataFrame: DataFrame with imputed data.
    
    Raises:
        ValueError: If the imputation strategy is unsupported.
    """
    if strategy == 'mean':
        return df.fillna(df.mean())
    elif strategy == 'median':
        return df.fillna(df.median())
    else:
        raise ValueError(f"Unsupported imputation strategy: {strategy}")