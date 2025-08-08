import numpy as np
import pandas as pd
from phenoqc.missing_data import ImputationEngine


def test_knn_tuning_selects_candidate():
    # Simple numeric frame; the actual best k may vary slightly, so just check tuning summary structure
    rng = np.random.RandomState(0)
    base = rng.normal(size=(50, 3))
    df = pd.DataFrame(base, columns=['a', 'b', 'c'])
    df.iloc[::7, 1] = np.nan

    cfg = {
        'strategy': 'knn',
        'params': {},
        'tuning': {
            'enable': True,
            'mask_fraction': 0.1,
            'scoring': 'MAE',
            'max_cells': 2000,
            'random_state': 42,
            'grid': {'n_neighbors': [3, 5]}
        }
    }
    eng = ImputationEngine(cfg)
    out = eng.fit_transform(df)
    assert eng.tuning_summary is not None and eng.tuning_summary.get('enabled') is True
    # n_neighbors chosen must be from the grid
    assert eng.tuning_summary.get('best', {}).get('n_neighbors') in [3, 5, None]

