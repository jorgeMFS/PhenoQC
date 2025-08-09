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

