import pandas as pd
import hashlib
import collections
from dataclasses import dataclass
from typing import List, Optional, Dict, Any
import numpy as np
from scipy import stats
from .missing_data import ImputationEngine

# Central list of quality metric identifiers used across the application.
QUALITY_METRIC_CHOICES = [
    "accuracy",
    "redundancy",
    "traceability",
    "timeliness",
    "imputation_bias",
]


def check_accuracy(df: pd.DataFrame, schema_cfg: Dict[str, Any]) -> pd.DataFrame:
    """Check values fall within schema-defined ranges.

    Parameters
    ----------
    df : pd.DataFrame
        Data to evaluate.
    schema_cfg : dict
        JSON schema configuration containing ``properties`` with optional
        ``minimum`` and ``maximum`` keys per column.

    Returns
    -------
    pd.DataFrame
        Rows with values outside the allowed range. Columns include the
        offending column name and bounds for easier debugging. Empty DataFrame
        if no issues are found.
    """
    records = []
    seen_pairs = set()
    props = schema_cfg.get("properties", {})
    for col, rules in props.items():
        if col not in df.columns:
            continue
        min_val = rules.get("minimum")
        max_val = rules.get("maximum")
        if min_val is None and max_val is None:
            continue
        series = pd.to_numeric(df[col], errors="coerce")
        mask = pd.Series(False, index=series.index)
        if min_val is not None:
            mask |= series < min_val
        if max_val is not None:
            mask |= series > max_val
        offending = df.loc[mask, col]
        records.extend(
            [
                {
                    "row": idx,
                    "column": col,
                    "value": value,
                    "minimum": min_val,
                    "maximum": max_val,
                }
                for idx, value in offending.items()
            ]
        )
    return pd.DataFrame(records)


def detect_redundancy(df: pd.DataFrame, threshold: float = 0.98, method: str = 'pearson') -> pd.DataFrame:
    """Detect highly correlated or identical columns.

    Numeric columns are checked using Pearson correlation. Any pair with an
    absolute correlation >= ``threshold`` is reported. All columns (including
    non-numeric) are additionally hashed using SHA-256 to identify identical
    columns.

    Parameters
    ----------
    df : pd.DataFrame
        Input data.
    threshold : float, optional
        Correlation threshold above which columns are flagged, by default 0.98.

    Returns
    -------
    pd.DataFrame
        A DataFrame with columns ``column_1``, ``column_2``, ``metric`` and
        ``value`` describing redundant column pairs. Empty if none detected.
    """
    records = []
    seen_pairs = set()

    # Correlation for numeric columns using selected method ('pearson' or 'spearman')
    numeric_cols = df.select_dtypes(include="number").columns
    if len(numeric_cols) >= 2:
        try:
            corr = df[numeric_cols].corr(method=str(method or 'pearson')).abs()
        except Exception:
            corr = df[numeric_cols].corr(method="pearson").abs()
        for i, col1 in enumerate(numeric_cols):
            for col2 in numeric_cols[i + 1:]:
                val = corr.loc[col1, col2]
                if pd.notna(val) and val >= threshold:
                    key = (col1, col2)
                    seen_pairs.add(key)
                    records.append({
                        "column_1": col1,
                        "column_2": col2,
                        "metric": "correlation",
                        "value": float(val),
                    })

    # Hash-based check for identical columns
    hashes: Dict[str, List[str]] = {}
    for col in df.columns:
        series_hash = hashlib.sha256(
            pd.util.hash_pandas_object(df[col], index=False).values.tobytes()
        ).hexdigest()
        hashes.setdefault(series_hash, []).append(col)

    for cols in hashes.values():
        if len(cols) > 1:
            first = cols[0]
            for other in cols[1:]:
                key = (first, other) if first < other else (other, first)
                # Prefer marking as identical; skip if already recorded as correlation
                if key in seen_pairs:
                    # Replace the existing correlation entry with identical
                    for rec in records:
                        if {rec.get("column_1"), rec.get("column_2")} == set(key):
                            rec["metric"] = "identical"
                            rec["value"] = 1.0
                    continue
                records.append({
                    "column_1": first,
                    "column_2": other,
                    "metric": "identical",
                    "value": 1.0,
                })
    return pd.DataFrame(records)


def check_traceability(
    df: pd.DataFrame, id_cols: List[str], source_col: Optional[str] = None
) -> pd.DataFrame:
    """Ensure identifiers are unique and traceable.

    Parameters
    ----------
    df : pd.DataFrame
        Dataset to validate.
    id_cols : list of str
        Columns that should uniquely identify a record. Must not be empty.
    source_col : str, optional
        Column expected to contain non-null provenance information.

    Returns
    -------
    pd.DataFrame
        Rows that violate traceability requirements with an ``issue`` column
        describing the problem.

    Raises
    ------
    ValueError
        If ``id_cols`` is empty.
    """
    if not id_cols:
        raise ValueError("id_cols must contain at least one column")

    records = []
    # Check for missing IDs or duplicates across id_cols combination
    dup_mask = df.duplicated(subset=id_cols, keep=False)
    if dup_mask.any():
        records.extend(
            {"row": idx, "issue": "duplicate_identifier"}
            for idx in df.index[dup_mask]
        )
    missing_mask = df[id_cols].isnull().any(axis=1)
    records.extend(
        {"row": idx, "issue": "missing_identifier"}
        for idx in df.index[missing_mask]
    )
    # Source column must be present and non-null if provided
    if source_col and source_col in df.columns:
        src_null = df[source_col].isnull()
        for idx in df.index[src_null]:
            records.append({"row": idx, "issue": "missing_source"})
    return pd.DataFrame(records)


def check_timeliness(df: pd.DataFrame, date_col: str, max_lag_days: int) -> pd.DataFrame:
    """Return records with outdated or invalid timestamps.

    Parameters
    ----------
    df : pd.DataFrame
        Input data.
    date_col : str
        Column containing datetime strings.
    max_lag_days : int
        Maximum allowed age in days.

    Returns
    -------
    pd.DataFrame
        Rows where ``now - date_col`` exceeds ``max_lag_days`` or where
        ``date_col`` is missing/invalid. An ``issue`` column describes the
        problem.
    """
    if date_col not in df.columns:
        return df.iloc[0:0].copy()
    dates = pd.to_datetime(df[date_col], errors="coerce")
    lag = pd.Timedelta(days=max_lag_days)
    stale_mask = (pd.Timestamp.now() - dates) > lag
    invalid_mask = dates.isna()
    results = []

    def _tag_issue(mask: pd.Series, label: str) -> None:
        if mask.any():
            subset = df.loc[mask].copy()
            subset["issue"] = label
            results.append(subset)

    _tag_issue(stale_mask, "lag_exceeded")
    _tag_issue(invalid_mask, "missing_or_invalid_date")
    return pd.concat(results) if results else df.iloc[0:0].copy()


# -----------------------------
# Class distribution (imbalance)
# -----------------------------

@dataclass
class ClassDistributionResult:
    counts: Dict[str, int]
    proportions: Dict[str, float]
    minority_class: Optional[str]
    minority_prop: Optional[float]
    warn_threshold: float
    warning: bool


def report_class_distribution(
    df: pd.DataFrame,
    label_column: str,
    warn_threshold: float = 0.10,
) -> ClassDistributionResult:
    if label_column not in df.columns:
        return ClassDistributionResult({}, {}, None, None, warn_threshold, False)
    series = df[label_column].dropna().astype(str)
    counts = series.value_counts().to_dict()
    total = sum(counts.values())
    proportions = {k: (v / total) for k, v in counts.items()} if total else {}
    if proportions:
        minority_class, minority_prop = min(proportions.items(), key=lambda kv: kv[1])
    else:
        minority_class, minority_prop = None, None
    warning_flag = bool(minority_prop is not None and minority_prop < warn_threshold)
    return ClassDistributionResult(
        counts=counts,
        proportions=proportions,
        minority_class=minority_class,
        minority_prop=minority_prop,
        warn_threshold=warn_threshold,
        warning=warning_flag,
    )


class ClassCounter:
    """Chunk-friendly class counter for streaming aggregation."""

    def __init__(self) -> None:
        self._counts = collections.Counter()
        self._n = 0

    def update(self, series: pd.Series) -> None:
        if series is None:
            return
        s = series.dropna().astype(str)
        if not s.empty:
            self._counts.update(s)
            self._n += len(s)

    def finalize(self, warn_threshold: float = 0.10) -> ClassDistributionResult:
        counts = dict(self._counts)
        total = sum(counts.values())
        proportions = {k: (v / total) for k, v in counts.items()} if total else {}
        if proportions:
            minority_class, minority_prop = min(proportions.items(), key=lambda kv: kv[1])
        else:
            minority_class, minority_prop = None, None
        warning_flag = bool(minority_prop is not None and minority_prop < warn_threshold)
        return ClassDistributionResult(
            counts=counts,
            proportions=proportions,
            minority_class=minority_class,
            minority_prop=minority_prop,
            warn_threshold=warn_threshold,
            warning=warning_flag,
        )

@dataclass
class BiasRow:
    column: str
    n_obs: int
    n_imp: int
    mean_diff: float
    smd: float
    var_ratio: float
    ks_stat: Optional[float]
    ks_p: Optional[float]
    warn: bool


def imputation_bias_report(
    original_df: pd.DataFrame,
    imputed_df: pd.DataFrame,
    imputation_mask: Dict[str, pd.Series],
    columns: Optional[List[str]] = None,
    smd_threshold: float = 0.10,
    var_ratio_low: float = 0.5,
    var_ratio_high: float = 2.0,
    ks_alpha: float = 0.05,
) -> pd.DataFrame:
    cols = columns or imputed_df.select_dtypes(include=[np.number]).columns.tolist()
    rows: List[BiasRow] = []

    for col in cols:
        mask = imputation_mask.get(col)
        if mask is None or col not in original_df.columns or col not in imputed_df.columns:
            continue
        try:
            obs = original_df.loc[~original_df[col].isna(), col].astype(float)
            imp = imputed_df.loc[mask.fillna(False), col].astype(float)
            post_all = imputed_df[col].dropna().astype(float)

            # Skip columns with no observed values or no imputed values
            if len(obs) == 0 or len(imp) == 0:
                continue

            low_n = bool(len(obs) < 10 or len(imp) < 10)
            if len(obs) < 3 or len(imp) < 3:
                ks_stat = None
                ks_p = None
            else:
                ks_stat, ks_p = stats.ks_2samp(obs.values, imp.values, alternative="two-sided", mode="auto")

            mean_diff = float(post_all.mean() - obs.mean()) if len(obs) and len(post_all) else np.nan
            obs_var = float(np.nanvar(obs, ddof=1)) if len(obs) > 1 else np.nan
            post_var = float(np.nanvar(post_all, ddof=1)) if len(post_all) > 1 else np.nan
            pooled_sd = np.sqrt(obs_var) if not np.isnan(obs_var) else np.nan
            smd = float((np.nanmean(imp) - np.nanmean(obs)) / pooled_sd) if pooled_sd and not np.isnan(pooled_sd) else np.nan
            var_ratio = (post_var / obs_var) if (obs_var not in (0.0, np.nan) and not np.isnan(obs_var) and not np.isnan(post_var)) else np.nan
            # Optional Wasserstein distance for distributional shift (not part of warn thresholds)
            try:
                wdist = float(stats.wasserstein_distance(obs.values, imp.values)) if len(obs) and len(imp) else np.nan
            except Exception:
                wdist = np.nan

            warn = (
                (not np.isnan(smd) and abs(smd) >= smd_threshold) or
                (not np.isnan(var_ratio) and (var_ratio <= var_ratio_low or var_ratio >= var_ratio_high)) or
                (ks_p is not None and ks_p < ks_alpha)
            )

            rows.append(
                BiasRow(
                    column=col,
                    n_obs=len(obs),
                    n_imp=len(imp),
                    mean_diff=mean_diff,
                    smd=float(smd) if not np.isnan(smd) else np.nan,
                    var_ratio=(
                        float(var_ratio) if not np.isnan(var_ratio) else np.nan
                    ),
                    ks_stat=float(ks_stat) if ks_stat is not None else None,
                    ks_p=float(ks_p) if ks_p is not None else None,
                    warn=warn,
                )
            )
            # Append auxiliary fields directly on dict export stage
            rows[-1].__dict__['wdist'] = wdist
            rows[-1].__dict__['low_n'] = low_n
        except Exception:
            continue

    df_out = pd.DataFrame([r.__dict__ for r in rows])
    if not df_out.empty:
        return df_out.sort_values(by=["warn", "column"], ascending=[False, True])
    return df_out


def imputation_stability_cv(
    df: pd.DataFrame,
    strategy: str,
    params: Optional[Dict[str, Any]] = None,
    repeats: int = 5,
    mask_fraction: float = 0.10,
    scoring: str = 'MAE',
    random_state: int = 42,
    columns: Optional[List[str]] = None,
) -> pd.DataFrame:
    """Estimate imputation repeatability via repeated mask-and-impute.

    For each repeat, randomly mask a fraction of observed numeric cells, impute,
    and compute an error per column on the masked positions using the chosen
    scoring rule ('MAE' or 'RMSE'). Aggregate per-column across repeats to
    obtain mean_error, sd_error, and a simple stability score (cv_error).

    Returns a DataFrame with columns: column, repeats, metric, mean_error,
    sd_error, cv_error.
    """
    if df is None or not isinstance(df, pd.DataFrame) or df.empty:
        return pd.DataFrame(columns=[
            'column', 'repeats', 'metric', 'mean_error', 'sd_error', 'cv_error'
        ])

    numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
    if columns is not None:
        numeric_cols = [c for c in numeric_cols if c in set(columns)]
    if not numeric_cols:
        return pd.DataFrame(columns=[
            'column', 'repeats', 'metric', 'mean_error', 'sd_error', 'cv_error'
        ])

    rng = np.random.RandomState(int(random_state))
    metric_name = str(scoring or 'MAE').upper()
    params = params or {}

    # Precompute observed mask and coordinates per column
    observed = df[numeric_cols].notna().to_numpy()
    coords_all = np.argwhere(observed)
    if coords_all.size == 0:
        return pd.DataFrame(columns=['column', 'repeats', 'metric', 'mean_error', 'sd_error', 'cv_error'])
    num_cells = coords_all.shape[0]
    sample_size = max(1, int(mask_fraction * num_cells))

    per_col_errors: Dict[str, list] = {c: [] for c in numeric_cols}

    for r in range(int(repeats)):
        try:
            idxs = rng.choice(num_cells, size=sample_size, replace=False)
            mask_coords = coords_all[idxs]
            masked = df[numeric_cols].copy()
            arr = masked.to_numpy().astype(float)
            original_vals = df[numeric_cols].to_numpy().astype(float).copy()
            # Build boolean mask for masked cells
            mask_bool = np.zeros_like(arr, dtype=bool)
            mask_bool[mask_coords[:, 0], mask_coords[:, 1]] = True
            arr[mask_bool] = np.nan
            masked.iloc[:, :] = arr

            # Impute using the provided strategy/params via ImputationEngine
            engine = ImputationEngine({'strategy': strategy, 'params': params})
            imputed = engine.fit_transform(masked)
            imputed_arr = imputed[numeric_cols].to_numpy().astype(float)

            # Compute error on masked positions per column
            for j, col in enumerate(numeric_cols):
                col_mask = mask_bool[:, j]
                if not np.any(col_mask):
                    continue
                true_vals = original_vals[:, j][col_mask]
                pred_vals = imputed_arr[:, j][col_mask]
                diffs = pred_vals - true_vals
                if metric_name == 'RMSE':
                    err = float(np.sqrt(np.mean(diffs ** 2)))
                else:
                    err = float(np.mean(np.abs(diffs)))
                if not np.isnan(err):
                    per_col_errors[col].append(err)
        except Exception:
            continue

    records = []
    for col in numeric_cols:
        errs = per_col_errors.get(col, [])
        if not errs:
            continue
        mean_err = float(np.mean(errs))
        sd_err = float(np.std(errs, ddof=1)) if len(errs) > 1 else 0.0
        cv_err = float(sd_err / mean_err) if mean_err != 0 else np.inf
        records.append({
            'column': col,
            'repeats': int(repeats),
            'metric': metric_name,
            'mean_error': mean_err,
            'sd_error': sd_err,
            'cv_error': cv_err,
        })

    df_out = pd.DataFrame(records)
    if not df_out.empty:
        return df_out.sort_values(by=['cv_error', 'mean_error'], ascending=[False, True])
    return df_out
