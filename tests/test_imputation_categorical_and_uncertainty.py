import numpy as np
import pandas as pd

from phenoqc.quality_metrics import imputation_bias_report, imputation_uncertainty_mice


def test_categorical_bias_reports_psi_and_cramers_v():
    # Construct a simple case where imputed differs from observed on categories
    orig = pd.DataFrame({
        'cat': ['A','A','B','B','B','C', None, None, None, None]
    })
    # Suppose imputed cells were mostly forced to 'A'
    imp_full = pd.DataFrame({
        'cat': ['A','A','B','B','B','C','A','A','A','A']
    })
    mask = {'cat': pd.Series([False, False, False, False, False, False, True, True, True, True])}

    out = imputation_bias_report(
        original_df=orig,
        imputed_df=imp_full,
        imputation_mask=mask,
        columns=['cat'],
        psi_threshold=0.01,
        cramer_threshold=0.05,
    )
    assert isinstance(out, pd.DataFrame)
    assert not out.empty
    # Must contain categorical metrics
    assert {'psi','cramers_v','chi2_p'}.issubset(out.columns)
    # We expect a warning due to distribution shift
    assert bool(out.loc[out['column']=='cat','warn'].iloc[0]) is True


def test_multiple_imputation_uncertainty_mice_returns_variance():
    rng = np.random.RandomState(0)
    n = 60
    x = rng.normal(0, 1, size=n)
    # Introduce missingness at random positions
    mask = rng.rand(n) < 0.3
    x_missing = x.copy()
    x_missing[mask] = np.nan
    df = pd.DataFrame({'x': x_missing, 'y': rng.normal(1, 2, size=n)})

    out = imputation_uncertainty_mice(df, repeats=3, mice_params={'max_iter': 5}, random_state=42)
    assert isinstance(out, pd.DataFrame)
    # If there were missing values, we should get a row and non-negative variance
    if mask.any():
        assert not out.empty
        assert 'mi_var' in out.columns and 'mi_std' in out.columns
        assert (out['mi_var'] >= 0).all()

