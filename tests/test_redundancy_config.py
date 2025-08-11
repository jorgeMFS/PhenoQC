import pandas as pd
import numpy as np

from phenoqc.quality_metrics import detect_redundancy
from phenoqc.validation import DataValidator


def test_detect_redundancy_accepts_method_and_threshold():
    n = 50
    a = np.arange(n, dtype=float)
    b = a * 2.0 + 1.0  # perfectly monotonic
    c = np.random.RandomState(0).normal(size=n)
    df = pd.DataFrame({"a": a, "b": b, "c": c})

    # Spearman or Pearson should both flag a-b near 1.0 correlation
    res_pearson = detect_redundancy(df, threshold=0.98, method="pearson")
    res_spearman = detect_redundancy(df, threshold=0.98, method="spearman")
    assert not res_pearson.empty
    assert not res_spearman.empty


def test_validation_uses_redundancy_params_from_config():
    # Build a simple DataFrame where two columns are highly correlated
    rng = np.random.RandomState(1)
    x = np.linspace(0, 10, 40)
    y = x + rng.normal(0, 0.01, size=len(x))
    df = pd.DataFrame({"id": np.arange(len(x)), "x": x, "y": y})
    schema = {"properties": {"id": {"type": "integer"}}}
    validator = DataValidator(df, schema, unique_identifiers=["id"])

    cfg = {
        "quality_metrics": ["redundancy"],
        "redundancy": {"threshold": 0.95, "method": "spearman"},
    }
    results = validator._apply_quality_metrics(cfg, {})
    red = results.get("Redundancy Issues")
    assert isinstance(red, pd.DataFrame)
    # Expect at least one redundancy pair reported
    assert not red.empty


