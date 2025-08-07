import pandas as pd
import pytest
from src.quality_metrics import (
    check_accuracy,
    detect_redundancy,
    check_traceability,
    check_timeliness,
)


def test_check_accuracy_flags_out_of_range():
    df = pd.DataFrame({"age": [10, 20, 5]})
    schema = {"properties": {"age": {"minimum": 8, "maximum": 18}}}
    result = check_accuracy(df, schema)
    # The function should flag row 1 (value 20) and row 2 (value 5) as out of range
    assert not result.empty
    assert set(result["row"]) == {1, 2}

def test_check_accuracy_all_within_range():
    df = pd.DataFrame({"age": [10, 12, 15]})
    schema = {"properties": {"age": {"minimum": 8, "maximum": 18}}}
    result = check_accuracy(df, schema)
    # All values are within the valid range, so the result should be empty
    assert result.empty


def test_detect_redundancy_identical_columns():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3], "c": [3, 4, 5]})
    result = detect_redundancy(df)
    assert ((result["metric"] == "identical") & (result["column_1"] == "a") & (result["column_2"] == "b")).any()

def test_detect_redundancy_highly_correlated_columns():
    # Create two highly correlated columns (not identical)
    a = [1, 2, 3, 4, 5]
    b = [2, 4, 6, 8, 10]  # Perfect correlation with a
    c = [5, 3, 6, 2, 1]   # Not correlated
    df = pd.DataFrame({"a": a, "b": b, "c": c})
    result = detect_redundancy(df)
    # Check for a correlation metric between a and b
    assert ((result["metric"] == "correlation") & 
            (((result["column_1"] == "a") & (result["column_2"] == "b")) | 
             ((result["column_1"] == "b") & (result["column_2"] == "a")))).any()


def test_check_traceability_missing_id():
    df = pd.DataFrame({"id": [1, None, 2], "source": ["x", "y", "z"]})
    result = check_traceability(df, ["id"])
    assert not result[result["issue"] == "missing_identifier"].empty


def test_detect_redundancy_high_correlation():
    df = pd.DataFrame({
        "a": [1, 2, 3, 4],
        "b": [2, 4, 6, 8],
        "c": [1, 3, 5, 7],
    })
    result = detect_redundancy(df)
    assert (
        (result["metric"] == "correlation")
        & (result["column_1"] == "a")
        & (result["column_2"] == "b")
    ).any()


def test_check_traceability_duplicates_and_missing_source():
    df = pd.DataFrame({
        "id": [1, 1, 2],
        "source": [None, "x", None],
    })
    result = check_traceability(df, ["id"], source_col="source")
    assert not result[result["issue"] == "duplicate_identifier"].empty
    assert not result[result["issue"] == "missing_source"].empty


def test_check_traceability_empty_id_cols_raises():
    df = pd.DataFrame({"id": [1, 2]})
    # When no identifier columns are provided the function should raise a
    # clear ``ValueError`` instead of failing inside ``pandas``.
    with pytest.raises(ValueError, match="id_cols must contain at least one column"):
        check_traceability(df, [])


def test_check_timeliness_flags_old_records():
    now = pd.Timestamp.now()
    old_date = (now - pd.Timedelta(days=10)).isoformat()
    recent_date = now.isoformat()
    df = pd.DataFrame({"timestamp": [old_date, recent_date]})
    result = check_timeliness(df, "timestamp", max_lag_days=5)
    # Only the outdated record should be flagged with appropriate issue
    assert old_date in result["timestamp"].values
    assert recent_date not in result["timestamp"].values
    assert (result[result["timestamp"] == old_date]["issue"] == "lag_exceeded").all()


def test_check_timeliness_flags_invalid_or_missing_dates():
    df = pd.DataFrame({"timestamp": ["not_a_date", None]})
    result = check_timeliness(df, "timestamp", max_lag_days=5)
    # Both rows should be reported with a missing/invalid date issue and the
    # offending rows should be preserved in the output.
    assert set(result.index) == {0, 1}
    assert result["issue"].eq("missing_or_invalid_date").all()
