import pandas as pd
from src.quality_metrics import (
    check_accuracy,
    detect_redundancy,
    check_traceability,
)


def test_check_accuracy_flags_out_of_range():
    df = pd.DataFrame({"age": [10, 20, 5]})
    schema = {"properties": {"age": {"minimum": 8, "maximum": 18}}}
    result = check_accuracy(df, schema)
    assert not result.empty
    assert set(result["row"]) == {1, 2}


def test_detect_redundancy_identical_columns():
    df = pd.DataFrame({"a": [1, 2, 3], "b": [1, 2, 3], "c": [3, 4, 5]})
    result = detect_redundancy(df)
    assert ((result["metric"] == "identical") & (result["column_1"] == "a") & (result["column_2"] == "b")).any()


def test_check_traceability_missing_id():
    df = pd.DataFrame({"id": [1, None, 2], "source": ["x", "y", "z"]})
    result = check_traceability(df, ["id"])
    assert not result[result["issue"] == "missing_identifier"].empty
