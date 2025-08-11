import numpy as np
import pandas as pd
from phenoqc.quality_metrics import imputation_stability_cv

def test_stability_cv_non_numeric_only():
    # DataFrame with only non-numeric columns
    df = pd.DataFrame({'a': ['foo', 'bar', 'baz'], 'b': ['x', 'y', 'z']})
    out = imputation_stability_cv(df, strategy='mean')
    assert isinstance(out, pd.DataFrame)
    assert out.empty

def test_stability_cv_basic_and_columns():
    rng = np.random.RandomState(42)
    n = 200
    df = pd.DataFrame({
        'x': rng.normal(0, 1.0, n),
        'y': rng.normal(5, 2.0, n),
        'z': rng.normal(10, 3.0, n),
    })
    # Introduce missingness
    for col in df.columns:
        idx = rng.choice(n, size=int(0.2*n), replace=False)
        df.loc[idx, col] = np.nan

    out = imputation_stability_cv(df, strategy='mean', repeats=4, mask_fraction=0.1, scoring='MAE', random_state=123)
    assert isinstance(out, pd.DataFrame)
    assert not out.empty
    assert {
        'column',
        'repeats',
        'metric',
        'mean_error',
        'sd_error',
        'cv_error',
    }.issubset(out.columns)


def test_stability_cv_columns_filtering():
    rng = np.random.RandomState(0)
    df = pd.DataFrame({
        'a': rng.normal(size=120),
        'b': rng.normal(size=120),
        'c': rng.normal(size=120),
    })
    # Mask some values to create missingness
    df.loc[:40, 'a'] = np.nan
    df.loc[20:70, 'b'] = np.nan
    res = imputation_stability_cv(df, strategy='mean', repeats=3, columns=['a','b'])
    assert set(res['column'].unique()).issubset({'a','b'})


def test_stability_cv_empty_df():
    empty = pd.DataFrame()
    out = imputation_stability_cv(empty, strategy='mean')
    assert isinstance(out, pd.DataFrame)
    assert out.empty


def test_stability_cv_all_missing_numeric():
    df = pd.DataFrame({'x': [np.nan, np.nan, np.nan]})
    out = imputation_stability_cv(df, strategy='mean')
    assert isinstance(out, pd.DataFrame)
    # No observed cells to mask => empty result
    assert out.empty


