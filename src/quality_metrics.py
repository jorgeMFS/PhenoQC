import pandas as pd
import hashlib
from typing import List, Optional, Dict, Any


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


def detect_redundancy(df: pd.DataFrame, threshold: float = 0.98) -> pd.DataFrame:
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

    # Pearson correlation for numeric columns
    numeric_cols = df.select_dtypes(include="number").columns
    if len(numeric_cols) >= 2:
        corr = df[numeric_cols].corr(method="pearson").abs()
        records.extend(
            {
                "column_1": col1,
                "column_2": col2,
                "metric": "correlation",
                "value": float(corr.loc[col1, col2]),
            }
            for i, col1 in enumerate(numeric_cols)
            for col2 in numeric_cols[i + 1:]
            if (
                pd.notna(corr.loc[col1, col2])
                and corr.loc[col1, col2] >= threshold
            )
        )

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
            records.extend(
                {
                    "column_1": first,
                    "column_2": other,
                    "metric": "identical",
                    "value": 1.0,
                }
                for other in cols[1:]
            )
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
