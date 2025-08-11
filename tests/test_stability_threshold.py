import numpy as np
import pandas as pd

from phenoqc.quality_metrics import imputation_stability_cv


def test_stability_cv_threshold_logic():
    # This test exercises the function only; threshold enforcement is in batch_processing
    df = pd.DataFrame({
        'a': [1.0, 2.0, np.nan, 4.0, 5.0],
        'b': [2.0, np.nan, 3.0, 4.0, 5.0],
    })
    res = imputation_stability_cv(df, strategy='mean', repeats=3, mask_fraction=0.5, scoring='MAE', random_state=0)
    assert isinstance(res, pd.DataFrame)
    assert not res.empty
    assert {'column','cv_error'}.issubset(res.columns)

