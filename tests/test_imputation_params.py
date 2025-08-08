import pandas as pd
from phenoqc.missing_data import ImputationEngine


def test_knn_params_passthrough():
    df = pd.DataFrame({
        'x': [1.0, None, 3.0, 4.0],
        'y': [2.0, 2.5, None, 5.0],
    })
    cfg = {
        'strategy': 'knn',
        'params': {'n_neighbors': 3},
        'tuning': {'enable': False},
    }
    eng = ImputationEngine(cfg)
    out = eng.fit_transform(df)
    assert out.isna().sum().sum() == 0

def test_knn_default_params():
    df = pd.DataFrame({
        'x': [1.0, None, 3.0, 4.0],
        'y': [2.0, 2.5, None, 5.0],
    })
    # params omitted
    cfg_missing = {
        'strategy': 'knn',
        'tuning': {'enable': False},
    }
    eng_missing = ImputationEngine(cfg_missing)
    out_missing = eng_missing.fit_transform(df)
    assert out_missing.isna().sum().sum() == 0

    # params empty
    cfg_empty = {
        'strategy': 'knn',
        'params': {},
        'tuning': {'enable': False},
    }
    eng_empty = ImputationEngine(cfg_empty)
    out_empty = eng_empty.fit_transform(df)
    assert out_empty.isna().sum().sum() == 0

