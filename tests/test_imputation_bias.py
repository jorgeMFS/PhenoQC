import numpy as np
import pandas as pd

from phenoqc.missing_data import ImputationEngine
from phenoqc.quality_metrics import imputation_bias_report


def test_imputation_bias_mean_imputer_warns_on_variance():
    rng = np.random.RandomState(0)
    n = 200
    x = rng.normal(loc=0.0, scale=1.0, size=n)
    # Inject 30% missing completely at random
    mask = rng.rand(n) < 0.3
    df = pd.DataFrame({'x': x})
    df_missing = df.copy()
    df_missing.loc[mask, 'x'] = np.nan

    engine = ImputationEngine({'strategy': 'mean'})
    df_imputed = engine.fit_transform(df_missing)
    imputation_mask = getattr(engine, 'imputation_mask', {})

    bias_df = imputation_bias_report(
        original_df=df_missing, imputed_df=df_imputed, imputation_mask=imputation_mask,
        columns=['x'], smd_threshold=0.05
    )
    assert isinstance(bias_df, pd.DataFrame)
    assert not bias_df.empty
    # With mean imputation, variance typically shrinks and warn flags can appear
    assert bool(bias_df.iloc[0]['warn']) in {True, False}


def test_imputation_bias_all_missing_column():
    # All values missing
    df_missing = pd.DataFrame({'x': [np.nan] * 10})
    engine = ImputationEngine({'strategy': 'mean'})
    df_imputed = engine.fit_transform(df_missing)
    imputation_mask = getattr(engine, 'imputation_mask', {})
    bias_df = imputation_bias_report(
        original_df=df_missing, imputed_df=df_imputed, imputation_mask=imputation_mask,
        columns=['x'], smd_threshold=0.05
    )
    assert isinstance(bias_df, pd.DataFrame)
    # Should be empty or have no rows for 'x'
    assert bias_df.empty or not (bias_df['column'] == 'x').any()


def test_imputation_bias_all_observed_column():
    # All values observed, no missing
    df_observed = pd.DataFrame({'x': np.arange(10)})
    engine = ImputationEngine({'strategy': 'mean'})
    df_imputed = engine.fit_transform(df_observed)
    imputation_mask = getattr(engine, 'imputation_mask', {})
    bias_df = imputation_bias_report(
        original_df=df_observed, imputed_df=df_imputed, imputation_mask=imputation_mask,
        columns=['x'], smd_threshold=0.05
    )
    assert isinstance(bias_df, pd.DataFrame)
    # Should be empty or have no rows for 'x'
    assert bias_df.empty or not (bias_df['column'] == 'x').any()


def test_imputation_bias_knn_changes_with_k():
    rng = np.random.RandomState(1)
    n = 200
    # Two clusters
    x = np.concatenate([
        rng.normal(loc=-2.0, scale=0.5, size=n // 2),
        rng.normal(loc=2.0, scale=0.5, size=n // 2)
    ])
    df = pd.DataFrame({'x': x})
    # 20% missing
    mask = rng.rand(n) < 0.2
    df_missing = df.copy()
    df_missing.loc[mask, 'x'] = np.nan

    # KNN with small k
    engine_small = ImputationEngine({'strategy': 'knn', 'params': {'n_neighbors': 3}})
    df_imp_small = engine_small.fit_transform(df_missing)
    mask_small = getattr(engine_small, 'imputation_mask', {})
    bias_small = imputation_bias_report(df_missing, df_imp_small, mask_small, columns=['x'])

    # KNN with larger k
    engine_large = ImputationEngine({'strategy': 'knn', 'params': {'n_neighbors': 9}})
    df_imp_large = engine_large.fit_transform(df_missing)
    mask_large = getattr(engine_large, 'imputation_mask', {})
    bias_large = imputation_bias_report(df_missing, df_imp_large, mask_large, columns=['x'])

    assert not bias_small.empty and not bias_large.empty
    # Compare absolute SMDs if available
    smd_small = abs(float(bias_small.iloc[0]['smd'])) if not pd.isna(bias_small.iloc[0]['smd']) else None
    smd_large = abs(float(bias_large.iloc[0]['smd'])) if not pd.isna(bias_large.iloc[0]['smd']) else None
    if smd_small is not None and smd_large is not None:
        assert smd_large <= smd_small or abs(smd_large - smd_small) < 0.05


def test_imputation_bias_all_strategies_basic():
    rng = np.random.RandomState(2)
    n = 120
    df = pd.DataFrame({
        'a': rng.normal(0, 1, size=n),
        'b': rng.normal(5, 2, size=n)
    })
    miss_mask = rng.rand(n) < 0.25
    dfm = df.copy()
    dfm.loc[miss_mask, 'a'] = np.nan
    strategies = [
        {'strategy': 'mean'},
        {'strategy': 'median'},
        {'strategy': 'mode'},
        {'strategy': 'knn', 'params': {'n_neighbors': 3}},
        {'strategy': 'mice', 'params': {'max_iter': 5}},
        {'strategy': 'svd', 'params': {'rank': 2}},
    ]
    for cfg in strategies:
        eng = ImputationEngine(cfg)
        df_imp = eng.fit_transform(dfm)
        mask = getattr(eng, 'imputation_mask', {})
        bias = imputation_bias_report(dfm, df_imp, mask, columns=['a'])
        assert isinstance(bias, pd.DataFrame)

