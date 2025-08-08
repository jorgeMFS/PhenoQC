import pandas as pd
import numpy as np
import logging
from sklearn.experimental import enable_iterative_imputer
from sklearn.impute import KNNImputer, IterativeImputer
try:
    from fancyimpute import IterativeSVD
    _HAS_FANCY = True
except Exception:
    IterativeSVD = None
    _HAS_FANCY = False

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


class ImputationEngine:
    """Configuration-driven imputation with optional quick tuning.

    Expected cfg structure:
    {
      'strategy': 'knn' | 'mean' | 'median' | 'mode' | 'mice' | 'svd',
      'params': { ... },                 # optional passthrough
      'per_column': { 'col': { 'strategy': 'mice', 'params': {...} } },
      'tuning': {
          'enable': bool,
          'mask_fraction': 0.1,
          'scoring': 'MAE' | 'RMSE',
          'max_cells': 50000,
          'random_state': 42,
          'grid': { 'n_neighbors': [3,5,7] }
      }
    }
    """

    def __init__(self, cfg: dict | None, exclude_columns: list[str] | None = None) -> None:
        self.cfg = cfg or {}
        self.exclude_columns = set(exclude_columns or [])
        self.chosen_params: dict = {}
        self.tuning_summary: dict | None = None
        self._tuned_once: bool = False

    def _numeric_columns(self, df: pd.DataFrame) -> list[str]:
        cols = df.select_dtypes(include=['number']).columns.tolist()
        return [c for c in cols if c not in self.exclude_columns]

    def _apply_simple(self, df: pd.DataFrame, cols: list[str], strategy: str, params: dict | None) -> pd.DataFrame:
        result = df.copy()
        if strategy == 'mean':
            for c in cols:
                if pd.api.types.is_numeric_dtype(result[c]):
                    result[c] = result[c].fillna(result[c].mean())
        elif strategy == 'median':
            for c in cols:
                if pd.api.types.is_numeric_dtype(result[c]):
                    result[c] = result[c].fillna(result[c].median())
        elif strategy == 'mode':
            for c in cols:
                mode_vals = result[c].mode(dropna=True)
                if not mode_vals.empty:
                    result[c] = result[c].fillna(mode_vals[0])
        elif strategy == 'knn':
            if cols:
                imputer = KNNImputer(**(params or {}))
                result[cols] = imputer.fit_transform(result[cols])
        elif strategy == 'mice':
            if cols:
                imputer = IterativeImputer(**(params or {}))
                result[cols] = imputer.fit_transform(result[cols])
        elif strategy == 'svd':
            if not _HAS_FANCY:
                logging.warning("IterativeSVD (fancyimpute) not available; falling back to mean")
                return self._apply_simple(result, cols, 'mean', None)
            if cols:
                # Ensure rank is valid
                n_rows, n_cols = result[cols].shape
                k = min(n_rows, n_cols) - 1
                if k < 1:
                    logging.warning("Insufficient dimensions for SVD; falling back to mean")
                    return self._apply_simple(result, cols, 'mean', None)
                imputer = IterativeSVD(**(params or {}))
                result[cols] = imputer.fit_transform(result[cols])
        else:
            logging.warning(f"Unknown strategy '{strategy}', skipping.")
        return result

    def _score_imputation(self, original: pd.DataFrame, imputed: pd.DataFrame, mask_positions: np.ndarray, metric: str) -> float:
        # mask_positions is boolean mask for cells that were masked
        diff = (original - imputed).to_numpy()
        masked_values = diff[mask_positions]
        if masked_values.size == 0:
            return np.inf
        if metric.upper() == 'RMSE':
            return float(np.sqrt(np.mean(masked_values ** 2)))
        return float(np.mean(np.abs(masked_values)))

    def _tune_knn(self, df_sub: pd.DataFrame, tuning: dict) -> dict:
        rng = np.random.RandomState(int(tuning.get('random_state', 42)))
        scoring = str(tuning.get('scoring', 'MAE')).upper()
        mask_fraction = float(tuning.get('mask_fraction', 0.1))
        max_cells = int(tuning.get('max_cells', 50000))
        grid = tuning.get('grid', {}) or {}
        candidates = grid.get('n_neighbors', [3, 5, 7])

        observed = df_sub.notna().to_numpy()
        coords = np.argwhere(observed)
        if coords.size == 0:
            return {'n_neighbors': None, 'score': np.inf, 'metric': scoring}
        sample_size = min(max_cells, coords.shape[0], int(max(1, mask_fraction * coords.shape[0])))
        idxs = rng.choice(coords.shape[0], size=sample_size, replace=False)
        mask_coords = coords[idxs]

        best = {'n_neighbors': None, 'score': np.inf, 'metric': scoring}
        for k in candidates:
            imputer = KNNImputer(n_neighbors=int(k))
            # Build masked copy
            masked = df_sub.copy()
            mask_bool = np.zeros_like(masked.to_numpy(), dtype=bool)
            mask_bool[mask_coords[:, 0], mask_coords[:, 1]] = True
            original_vals = masked.to_numpy().copy()
            arr = masked.to_numpy()
            arr[mask_bool] = np.nan
            masked.iloc[:, :] = arr
            imputed_arr = imputer.fit_transform(masked)
            imputed = pd.DataFrame(imputed_arr, columns=masked.columns, index=masked.index)
            score = self._score_imputation(pd.DataFrame(original_vals, columns=masked.columns, index=masked.index), imputed, mask_bool, scoring)
            if score < best['score']:
                best = {'n_neighbors': int(k), 'score': float(score), 'metric': scoring}
        return best

    def fit_transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply configured imputation to a copied DataFrame.

        - Excludes columns listed in exclude_columns from numeric imputation matrix
        - Applies per-column overrides when provided
        - Optionally tunes KNN parameters using mask-and-score
        """
        cfg = self.cfg or {}
        global_strategy = cfg.get('strategy') or 'none'
        global_params = cfg.get('params') or {}
        per_column = cfg.get('per_column') or {}
        tuning = cfg.get('tuning') or {}

        result = df.copy()
        numeric_cols = self._numeric_columns(result)

        # Optional tuning for global KNN
        if (not self._tuned_once) and global_strategy == 'knn' and bool(tuning.get('enable', False)) and numeric_cols:
            best = self._tune_knn(result[numeric_cols], tuning)
            if best['n_neighbors'] is not None:
                global_params = {**global_params, 'n_neighbors': int(best['n_neighbors'])}
            self.tuning_summary = {'enabled': True, 'best': {'n_neighbors': best['n_neighbors']}, 'score': best['score'], 'metric': best['metric']}
            self._tuned_once = True
        elif bool(tuning.get('enable', False)):
            # Tuning requested but unsupported strategy
            self.tuning_summary = {'enabled': True, 'note': f"tuning not implemented for strategy '{global_strategy}'"}

        # Build strategy groups
        strategy_to_cols: dict[str, list[str]] = {}
        col_params: dict[str, dict] = {}
        for c in result.columns:
            if result[c].isna().sum() == 0:
                continue
            if c in per_column:
                strat = per_column[c].get('strategy', global_strategy)
                params = per_column[c].get('params', {})
            else:
                strat = global_strategy
                params = global_params
            strategy_to_cols.setdefault(strat, []).append(c)
            col_params[c] = params

        # Apply each strategy bucket
        for strat, cols in strategy_to_cols.items():
            # Separate numeric-only cols for advanced imputers
            if strat in ('knn', 'mice', 'svd'):
                cols = [c for c in cols if c in numeric_cols]
            params = None
            if strat not in ('mean', 'median', 'mode'):
                # For advanced, allow a single params dict; if per-column differs, prefer column-specific loop
                unique_param_sets = {tuple(sorted((col_params[c] or {}).items())) for c in cols}
                if len(unique_param_sets) <= 1:
                    params = next(iter(unique_param_sets), tuple())
                    params = dict(params)
                    result = self._apply_simple(result, cols, strat, params)
                else:
                    # Apply per column
                    for c in cols:
                        result = self._apply_simple(result, [c], strat, col_params.get(c) or {})
            else:
                result = self._apply_simple(result, cols, strat, None)

        self.chosen_params = {
            'global': {'strategy': global_strategy, 'params': global_params},
            'per_column': per_column,
        }
        return result
